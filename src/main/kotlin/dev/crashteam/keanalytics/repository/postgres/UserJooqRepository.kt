package dev.crashteam.keanalytics.repository.postgres

import dev.crashteam.keanalytics.db.model.enums.SubscriptionType
import dev.crashteam.keanalytics.db.model.tables.Users.USERS
import dev.crashteam.keanalytics.db.model.tables.pojos.Users
import dev.crashteam.keanalytics.domain.ApiKey
import org.jooq.DSLContext
import org.springframework.stereotype.Repository
import java.time.LocalDateTime

@Repository
class UserJooqRepository(
    private val dsl: DSLContext
) : UserRepository {

    override fun save(users: Users) {
        val u = USERS
        dsl.insertInto(
            u,
            u.USER_ID,
            u.API_KEY_PREFIX,
            u.API_KEY_KEY,
            u.API_KEY_HASH_KEY,
            u.SUBSCRIPTION_TYPE,
            u.SUBSCRIPTION_CREATED_AT,
            u.SUBSCRIPTION_END_AT,
            u.EMAIL,
            u.ROLE
        ).values(
            users.userId,
            users.apiKeyPrefix,
            users.apiKeyKey,
            users.apiKeyHashKey,
            users.subscriptionType,
            users.subscriptionCreatedAt,
            users.subscriptionEndAt,
            users.email,
            users.role
        ).execute()
    }

    override fun updateSubscription(
        userId: String,
        userSubscription: SubscriptionType,
        createdAt: LocalDateTime,
        endAt: LocalDateTime
    ): Int {
        val u = USERS
        return dsl.update(u)
            .set(u.SUBSCRIPTION_TYPE, userSubscription)
            .set(u.SUBSCRIPTION_CREATED_AT, createdAt)
            .set(u.SUBSCRIPTION_END_AT, endAt)
            .where(u.USER_ID.eq(userId))
            .execute()
    }

    override fun updateApiKey(userId: String, apiKey: ApiKey): Int {
        val u = USERS
        return dsl.update(u)
            .set(u.API_KEY_KEY, apiKey.key)
            .set(u.API_KEY_HASH_KEY, apiKey.hashKey)
            .set(u.API_KEY_PREFIX, apiKey.prefix)
            .set(u.API_KEY_BLOCKED, apiKey.blocked)
            .where(u.USER_ID.eq(userId))
            .execute()
    }

    override fun findByUserId(userId: String): Users? {
        val u = USERS
        return dsl.selectFrom(u)
            .where(u.USER_ID.eq(userId))
            .fetchOneInto(Users::class.java)
    }

    override fun findByEmail(email: String): Users? {
        val u = USERS
        return dsl.selectFrom(u)
            .where(u.EMAIL.eq(email))
            .fetchOneInto(Users::class.java)
    }

    override fun findByApiKey_HashKey(hashKey: String): Users? {
        val u = USERS
        return dsl.selectFrom(u)
            .where(u.API_KEY_HASH_KEY.eq(hashKey))
            .fetchOneInto(Users::class.java)
    }
}
