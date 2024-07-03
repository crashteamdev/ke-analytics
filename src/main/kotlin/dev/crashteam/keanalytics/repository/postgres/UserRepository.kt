package dev.crashteam.keanalytics.repository.postgres

import dev.crashteam.keanalytics.db.model.enums.SubscriptionType
import dev.crashteam.keanalytics.db.model.tables.pojos.Users
import dev.crashteam.keanalytics.domain.ApiKey
import java.time.LocalDateTime

interface UserRepository {
    fun save(users: Users)

    fun findByUserId(userId: String): Users?

    fun findByEmail(email: String): Users?

    fun findByApiKey_HashKey(hashKey: String): Users?

    fun updateSubscription(
        userId: String,
        userSubscription: SubscriptionType,
        createdAt: LocalDateTime,
        endAt: LocalDateTime
    ): Int

    fun updateApiKey(
        userId: String,
        apiKey: ApiKey
    ): Int
}
