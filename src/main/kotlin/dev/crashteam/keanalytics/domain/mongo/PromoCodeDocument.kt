package dev.crashteam.keanalytics.domain.mongo

import org.bson.types.ObjectId
import org.springframework.data.mongodb.core.index.Indexed
import org.springframework.data.mongodb.core.mapping.Document
import org.springframework.data.mongodb.core.mapping.MongoId
import java.time.LocalDateTime

@Document("promocode")
data class PromoCodeDocument(
    @Indexed(unique = true)
    val code: String,
    val description: String,
    val validUntil: LocalDateTime,
    val numberOfUses: Int,
    val useLimit: Int,
    val type: PromoCodeType,
    val discount: Short? = null,
    val additionalDays: Int? = null,

    @MongoId
    val id: ObjectId = ObjectId(),
)

enum class PromoCodeType {
    DISCOUNT, ADDITIONAL_DAYS
}
