package dev.crashteam.keanalytics.service

import dev.crashteam.keanalytics.db.model.enums.SubscriptionType
import dev.crashteam.keanalytics.db.model.tables.pojos.Users
import dev.crashteam.keanalytics.service.exception.UserSubscriptionGiveawayException
import org.springframework.stereotype.Service
import java.time.LocalDateTime

@Service
class UserSubscriptionService(
    private val userRepository: dev.crashteam.keanalytics.repository.postgres.UserRepository,
) {

    fun giveawayDemoSubscription(userId: String, days: Long) {
        val user = userRepository.findByUserId(userId)
        if (user?.subscriptionType != null) {
            throw UserSubscriptionGiveawayException("User $userId already had subscription")
        }
        if (user == null) {
            val newUser = Users().apply {
                this.userId = userId
                this.subscriptionCreatedAt = LocalDateTime.now()
                this.subscriptionType = SubscriptionType.demo
                this.subscriptionEndAt = this.subscriptionCreatedAt.plusDays(days)
            }
            userRepository.save(newUser)
        } else {
            userRepository.updateSubscription(
                userId,
                SubscriptionType.demo,
                LocalDateTime.now(),
                LocalDateTime.now().plusDays(days)
            )
        }
    }

    fun setUserSubscription(userId: String, subscriptionType: SubscriptionType, days: Long) {
        val user = userRepository.findByUserId(userId)
        if (user == null) {
            val newUser = Users().apply {
                this.userId = userId
                this.subscriptionCreatedAt = LocalDateTime.now()
                this.subscriptionType = subscriptionType
                this.subscriptionEndAt = this.subscriptionCreatedAt.plusDays(days)
            }
            userRepository.save(newUser)
        } else {
            val currentTime = LocalDateTime.now()
            val endAt = if (user.subscriptionEndAt != null && user.subscriptionEndAt.isAfter(currentTime)) {
                user.subscriptionEndAt.plusDays(days)
            } else {
                currentTime.plusDays(days)
            }
            userRepository.updateSubscription(
                userId,
                subscriptionType,
                currentTime,
                endAt
            )
        }
    }
}
