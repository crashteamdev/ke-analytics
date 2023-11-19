package dev.crashteam.keanalytics.domain.mongo

import org.bson.types.ObjectId
import org.springframework.data.domain.Range
import org.springframework.data.mongodb.core.index.Indexed
import org.springframework.data.mongodb.core.mapping.Document
import org.springframework.data.mongodb.core.mapping.MongoId
import java.time.LocalDate
import java.time.LocalDateTime

@Document("users")
data class UserDocument(
    @Indexed(unique = true)
    val userId: String,
    val subscription: SubscriptionDocument? = null,
    val apiKey: ApiKey? = null,
    val email: String? = null,
    val role: UserRole? = null,
    val lastUsageDay: LocalDate? = null,

    @MongoId
    val id: ObjectId = ObjectId(),
)

data class ApiKey(
    val prefix: String,
    val key: String,
    val hashKey: String,
    val blocked: Boolean,
)

data class SubscriptionDocument(
    val type: SubscriptionType? = null, // Deprecated
    val subType: String?,
    val createdAt: LocalDateTime,
    val endAt: LocalDateTime,

    @MongoId
    val id: ObjectId = ObjectId(),
)

enum class UserRole {
    ADMIN
}

sealed class UserSubscription(val num: Int, val name: String) {
    abstract fun days(): Range<Int>
    abstract fun shopReports(): Int
    abstract fun categoryReports(): Int
    abstract fun price(): Int
}

object DefaultSubscription : UserSubscription(1, "default") {
    override fun days(): Range<Int> {
        return Range.closed(1, 30)
    }

    override fun shopReports(): Int {
        return 3
    }

    override fun categoryReports(): Int {
        return 0
    }

    override fun price(): Int {
        return 1470
    }
}

object AdvancedSubscription : UserSubscription(2, "advanced") {
    override fun days(): Range<Int> {
        return Range.closed(1, 90)
    }

    override fun shopReports(): Int {
        return 6
    }

    override fun categoryReports(): Int {
        return 2
    }

    override fun price(): Int {
        return 2190
    }
}

object ProSubscription : UserSubscription(3, "pro") {
    override fun days(): Range<Int> {
        return Range.closed(1, 120)
    }

    override fun shopReports(): Int {
        return 15
    }

    override fun categoryReports(): Int {
        return 4
    }

    override fun price(): Int {
        return 3200
    }
}

enum class SubscriptionType(val num: Int) {
    THIRTY_DAYS(1);

    companion object {
        fun findByNum(num: Int): SubscriptionType? {
            return values().find { it.num == num }
        }
    }
}
