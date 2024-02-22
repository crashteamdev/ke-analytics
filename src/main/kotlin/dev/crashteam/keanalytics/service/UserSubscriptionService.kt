package dev.crashteam.keanalytics.service

import dev.crashteam.keanalytics.domain.mongo.SubscriptionDocument
import dev.crashteam.keanalytics.repository.mongo.UserRepository
import dev.crashteam.keanalytics.service.exception.UserSubscriptionGiveawayException
import kotlinx.coroutines.reactor.awaitSingleOrNull
import org.springframework.stereotype.Service
import java.time.LocalDate

@Service
class UserSubscriptionService(
    private val userRepository: UserRepository,
) {

    suspend fun giveawayDemoSubscription(userId: String) {
        val userDocument = userRepository.findByUserId(userId).awaitSingleOrNull()
            ?: throw UserSubscriptionGiveawayException("User $userId not found")
        if (userDocument.subscription != null) {
            throw UserSubscriptionGiveawayException("User $userId already had subscription")
        }
        val updateUserDocument = userDocument.copy(
            subscription = SubscriptionDocument(
                subType = "demo",
                createdAt = LocalDate.now().atStartOfDay(),
                endAt = LocalDate.now().atStartOfDay().plusDays(5)
            )
        )
        userRepository.save(updateUserDocument).awaitSingleOrNull()
    }

}
