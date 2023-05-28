package dev.crashteam.keanalytics.service

import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import mu.KotlinLogging
import dev.crashteam.keanalytics.client.yookassa.YooKassaClient
import dev.crashteam.keanalytics.client.yookassa.model.PaymentAmount
import dev.crashteam.keanalytics.client.yookassa.model.PaymentConfirmation
import dev.crashteam.keanalytics.client.yookassa.model.PaymentRequest
import dev.crashteam.keanalytics.client.yookassa.model.PaymentResponse
import dev.crashteam.keanalytics.domain.mongo.PaymentDocument
import dev.crashteam.keanalytics.domain.mongo.SubscriptionDocument
import dev.crashteam.keanalytics.domain.mongo.UserDocument
import dev.crashteam.keanalytics.domain.mongo.UserSubscription
import dev.crashteam.keanalytics.extensions.mapToSubscription
import dev.crashteam.keanalytics.repository.mongo.PaymentRepository
import dev.crashteam.keanalytics.repository.mongo.ReferralCodeRepository
import dev.crashteam.keanalytics.repository.mongo.UserRepository
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import reactor.core.publisher.Mono
import java.math.BigDecimal
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import java.util.*

private val log = KotlinLogging.logger {}

@Service
class PaymentService(
    val paymentRepository: PaymentRepository,
    val userRepository: UserRepository,
    val referralCodeRepository: ReferralCodeRepository,
    val youKassaClient: YooKassaClient
) {

    suspend fun findPayment(paymentId: String): PaymentDocument? {
        return paymentRepository.findByPaymentId(paymentId).awaitSingleOrNull()
    }

    suspend fun savePayment(
        paymentId: String,
        userId: String,
        status: String,
        paid: Boolean,
        amount: String,
        subscription: Int,
        multiply: Short,
        referralCode: String? = null,
    ): Mono<PaymentDocument> {
        val paymentDocument = PaymentDocument(
            paymentId,
            userId,
            status,
            paid,
            BigDecimal(amount).setScale(2),
            subscription,
            multiply = multiply,
            referralCode = referralCode,
            createdAt = LocalDateTime.now()
        )
        log.info { "Save initial payment document $paymentDocument" }
        return paymentRepository.save(paymentDocument)
    }

    @Transactional
    suspend fun saveUserWithSubscription(
        paymentId: String,
        userId: String,
        user: UserDocument?,
        userSubscription: UserSubscription,
        subscriptionDays: Long,
        paymentPaid: Boolean,
        paymentStatus: String,
        referralCode: String?
    ) {
        val saveUser = if (user == null) {
            UserDocument(
                userId,
                SubscriptionDocument(
                    subType = userSubscription.name,
                    createdAt = LocalDateTime.now(),
                    endAt = LocalDateTime.now().plusDays(subscriptionDays)
                )
            )
        } else {
            val currentTime = LocalDateTime.now()
            val endAt = if (user.subscription?.endAt != null && user.subscription.endAt.isAfter(currentTime)) {
                user.subscription.endAt.plusDays(subscriptionDays)
            } else LocalDateTime.now().plusDays(subscriptionDays)
            user.copy(
                subscription = SubscriptionDocument(
                    subType = userSubscription.name,
                    createdAt = LocalDateTime.now(),
                    endAt = endAt
                )
            )
        }
        userRepository.save(saveUser).awaitSingleOrNull()

        // Save payment
        val payment = paymentRepository.findByPaymentId(paymentId).awaitSingle()
        val updatedPayment = payment.copy(paid = paymentPaid, status = paymentStatus)
        paymentRepository.save(updatedPayment).awaitSingleOrNull()

        // Save if user was invited
        if (referralCode != null) {
            val referralCodeDocument = referralCodeRepository.findByCode(referralCode).awaitSingleOrNull()
            if (referralCodeDocument != null) {
                referralCodeRepository.addReferralCodeUser(referralCode, userId, userSubscription.name)
                    .awaitSingleOrNull()
            }
        }
    }

    @Transactional
    suspend fun upgradeUserSubscription(
        user: UserDocument,
        userSubscription: UserSubscription,
        paymentId: String,
        paymentPaid: Boolean,
        paymentStatus: String
    ) {
        val upgradeUser = user.copy(
            subscription = user.subscription!!.copy(subType = userSubscription.name)
        )
        userRepository.save(upgradeUser).awaitSingleOrNull()
        val payment = paymentRepository.findByPaymentId(paymentId).awaitSingle()
        val updatedPayment = payment.copy(paid = paymentPaid, status = paymentStatus)
        paymentRepository.save(updatedPayment).awaitSingleOrNull()
    }

    suspend fun createUpgradeUserSubscriptonPayment(
        userDocument: UserDocument,
        upgradeTarget: UserSubscription,
        paymentRedirectUrl: String
    ): PaymentResponse {
        val currentUserSubscription = userDocument.subscription!!
        val daysLeft = ChronoUnit.DAYS.between(LocalDateTime.now(), currentUserSubscription.endAt)
        val currentUserSub = currentUserSubscription.subType?.mapToSubscription()!!
        val alreadyPayed = currentUserSub.price() - (currentUserSub.price() / 30) * (30 - daysLeft)
        val newSubPrice = (upgradeTarget.price() / 30) * daysLeft
        val upgradePrice = newSubPrice - alreadyPayed

        val paymentRequest = PaymentRequest(
            amount = PaymentAmount(
                upgradePrice.toString(),
                "RUB"
            ),
            capture = true,
            confirmation = PaymentConfirmation("redirect", paymentRedirectUrl),
            createdAt = LocalDateTime.now(),
            description = "Upgrade subscription from ${currentUserSub.name} to ${upgradeTarget.name}",
            metadata = mapOf("sub_type" to upgradeTarget.name)
        )
        val paymentResponse = youKassaClient.createPayment(UUID.randomUUID().toString(), paymentRequest)
        val paymentDocument = PaymentDocument(
            paymentResponse.id,
            userDocument.userId,
            paymentResponse.status,
            paymentResponse.paid,
            BigDecimal(upgradePrice).setScale(2),
            upgradeTarget.num,
            daysLeft.toInt()
        )
        paymentRepository.save(paymentDocument).awaitSingleOrNull()

        return paymentResponse
    }
}
