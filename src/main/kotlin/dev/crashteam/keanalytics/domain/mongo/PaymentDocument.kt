package dev.crashteam.keanalytics.domain.mongo

import org.bson.types.ObjectId
import org.springframework.data.mongodb.core.index.Indexed
import org.springframework.data.mongodb.core.mapping.Document
import org.springframework.data.mongodb.core.mapping.MongoId
import java.math.BigDecimal
import java.time.LocalDateTime

@Document("payments")
data class PaymentDocument(
    @Indexed(unique = true)
    val paymentId: String,
    val userId: String,
    val status: String,
    val paid: Boolean,
    val amount: BigDecimal,
    val subscriptionType: Int,
    val daysPaid: Int? = null,
    val multiply: Short? = null,
    val referralCode: String? = null,
    val createdAt: LocalDateTime? = null,
    val currencyId: String? = null,

    @MongoId
    val id: ObjectId = ObjectId(),
)
