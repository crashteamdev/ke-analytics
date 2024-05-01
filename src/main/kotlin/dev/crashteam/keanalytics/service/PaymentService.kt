package dev.crashteam.keanalytics.service

import dev.crashteam.keanalytics.domain.mongo.*
import dev.crashteam.keanalytics.extensions.mapToSubscription
import dev.crashteam.keanalytics.repository.mongo.PaymentRepository
import dev.crashteam.keanalytics.repository.mongo.PromoCodeRepository
import dev.crashteam.keanalytics.repository.mongo.ReferralCodeRepository
import dev.crashteam.keanalytics.repository.mongo.UserRepository
import dev.crashteam.keanalytics.service.model.CallbackPaymentAdditionalInfo
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import mu.KotlinLogging
import org.springframework.data.mongodb.core.ReactiveMongoTemplate
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.Update
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import reactor.core.publisher.Mono
import java.math.BigDecimal
import java.time.LocalDateTime

private val log = KotlinLogging.logger {}

@Service
class PaymentService(
    val paymentRepository: PaymentRepository,
    val userRepository: UserRepository,
    val referralCodeRepository: ReferralCodeRepository,
    val promoCodeRepository: PromoCodeRepository,
    val reactiveMongoTemplate: ReactiveMongoTemplate,
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
        referralCode: String?,
        currencyId: String? = null,
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
        val updatedPayment = payment.copy(paid = paymentPaid, status = paymentStatus, currencyId = currencyId)
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

    @Transactional
    suspend fun callbackPayment(
        paymentId: String,
        userId: String,
        currencyId: String? = null,
        paymentAdditionalInfo: CallbackPaymentAdditionalInfo? = null,
    ) {
        val payment = findPayment(paymentId)!!
        val user = userRepository.findByUserId(userId).awaitSingleOrNull()
        val userSubscription = payment.subscriptionType.mapToSubscription()!!
        if (payment.daysPaid != null) {
            upgradeUserSubscription(
                user!!, userSubscription, paymentId, true, "success"
            )
        } else {
            var subDays = if (payment.multiply != null && payment.multiply > 1) {
                30 * payment.multiply
            } else 30
            if (paymentAdditionalInfo != null) {
                val additionalSubDays =
                    callbackPromoCode(paymentAdditionalInfo.promoCode, paymentAdditionalInfo.promoCodeType)
                subDays += additionalSubDays
            }
            if (userSubscription.price().toBigDecimal() != payment.amount && payment.multiply == null) {
                throw IllegalStateException(
                    "Wrong payment amount. subscriptionPrice=${userSubscription.price()};" +
                            " paymentAmount=${payment.amount}"
                )
            }
            saveUserWithSubscription(
                paymentId,
                userId,
                user,
                userSubscription,
                subDays.toLong(),
                true,
                "success",
                referralCode = payment.referralCode,
                currencyId = currencyId
            )
        }
    }

    private suspend fun callbackPromoCode(promoCode: String, promoCodeType: PromoCodeType): Int {
        return try {
            val additionalSubDays = if (promoCodeType == PromoCodeType.ADDITIONAL_DAYS) {
                val promoCodeDocument = promoCodeRepository.findByCode(promoCode).awaitSingleOrNull()
                promoCodeDocument?.additionalDays ?: 0
            } else 0
            val query = Query().apply { addCriteria(Criteria.where("code").`is`(promoCode)) }
            val update = Update().inc("numberOfUses", 1)
            reactiveMongoTemplate.findAndModify(query, update, PromoCodeDocument::class.java).awaitSingle()

            additionalSubDays
        } catch (e: Exception) {
            log.error(e) { "Failed to callback promoCode" }
            0
        }
    }

}
