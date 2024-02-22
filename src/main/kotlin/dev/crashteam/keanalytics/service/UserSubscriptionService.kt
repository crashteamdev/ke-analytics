package dev.crashteam.keanalytics.service

import dev.crashteam.keanalytics.domain.mongo.SubscriptionDocument
import dev.crashteam.keanalytics.domain.mongo.UserDocument
import dev.crashteam.keanalytics.repository.mongo.UserRepository
import dev.crashteam.keanalytics.service.exception.UserSubscriptionGiveawayException
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono
import java.time.LocalDate

@Service
class UserSubscriptionService(
    private val userRepository: UserRepository,
) {

    fun giveawayDemoSubscription(userId: String): Mono<UserDocument> {
        return userRepository.findByUserId(userId).flatMap { userDocument ->
            if (userDocument == null) {
                return@flatMap Mono.error(UserSubscriptionGiveawayException("User $userId not found"))
            }
            if (userDocument.subscription != null) {
                return@flatMap Mono.error(UserSubscriptionGiveawayException("User $userId already had subscription"))
            }
            val updateUserDocument = userDocument.copy(
                subscription = SubscriptionDocument(
                    subType = "demo",
                    createdAt = LocalDate.now().atStartOfDay(),
                    endAt = LocalDate.now().atStartOfDay().plusDays(5)
                )
            )
            userRepository.save(updateUserDocument)
        }
    }

}
