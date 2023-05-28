package dev.crashteam.keanalytics.client.yookassa.model

import com.fasterxml.jackson.annotation.JsonProperty
import dev.crashteam.keanalytics.client.yookassa.model.PaymentAmount
import java.time.LocalDateTime

data class PaymentStatusResponse(
    val id: String,
    val status: String,
    val paid: Boolean,
    val amount: PaymentAmount,
    @JsonProperty("created_at")
    val createdAt: LocalDateTime,
    val description: String,
    @JsonProperty("expires_at")
    val expiresAt: LocalDateTime?,
    val refundable: Boolean
)
