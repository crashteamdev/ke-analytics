package dev.crashteam.keanalytics.service

import dev.crashteam.keanalytics.db.model.tables.pojos.Payment
import dev.crashteam.keanalytics.db.model.tables.pojos.Users
import dev.crashteam.keanalytics.domain.UserSubscription
import dev.crashteam.keanalytics.extensions.mapToEntityUserSubscription
import dev.crashteam.keanalytics.extensions.mapToSubscription
import dev.crashteam.keanalytics.repository.postgres.PaymentRepository
import dev.crashteam.keanalytics.repository.postgres.UserRepository
import mu.KotlinLogging
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.time.LocalDateTime

private val log = KotlinLogging.logger {}

@Service
class PaymentService(
    private val paymentRepository: PaymentRepository,
    private val userRepository: UserRepository,
) {

    suspend fun findPayment(paymentId: String): Payment? {
        return paymentRepository.findByPaymentId(paymentId)
    }

    @Transactional
    suspend fun callbackPayment(
        paymentId: String,
        userId: String,
        currencyId: String? = null,
    ) {
        val payment = findPayment(paymentId)!!
        val user = userRepository.findByUserId(userId)
        val userSubscription = payment.subscriptionType.toInt().mapToSubscription()!!
        var subDays = if (payment.multiply != null && payment.multiply > 1) {
            30 * payment.multiply
        } else 30
        saveUserWithSubscription(
            paymentId,
            userId,
            user,
            userSubscription,
            subDays.toLong(),
            true,
            "success",
            currencyId = currencyId
        )
    }

    suspend fun saveUserWithSubscription(
        paymentId: String,
        userId: String,
        user: Users?,
        userSubscription: UserSubscription,
        subscriptionDays: Long,
        paymentPaid: Boolean,
        paymentStatus: String,
        currencyId: String?,
    ) {
        val subscriptionType = userSubscription.name.mapToEntityUserSubscription()
        if (user == null) {
            Users(
                userId,
                null,
                null,
                null,
                null,
                subscriptionType,
                LocalDateTime.now(),
                LocalDateTime.now().plusDays(subscriptionDays),
                null,
                null,
            )
        } else {
            val currentTime = LocalDateTime.now()
            val endAt = if (user.subscriptionEndAt != null && user.subscriptionEndAt.isAfter(currentTime)) {
                user.subscriptionEndAt.plusDays(subscriptionDays)
            } else LocalDateTime.now().plusDays(subscriptionDays)
            log.info {
                "User ${user.userId}. Subscription days: $subscriptionDays; " +
                        "End subscription date: $endAt. Old subscription end date=${user.subscriptionEndAt}"
            }
            userRepository.updateSubscription(userId, subscriptionType, LocalDateTime.now(), endAt)
        }
        paymentRepository.updatePaymentStatus(paymentId, paymentStatus, paymentPaid)
    }
}
