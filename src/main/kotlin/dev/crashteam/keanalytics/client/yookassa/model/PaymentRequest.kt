package dev.crashteam.keanalytics.client.yookassa.model

import com.fasterxml.jackson.annotation.JsonProperty
import dev.crashteam.keanalytics.client.yookassa.model.PaymentAmount
import dev.crashteam.keanalytics.client.yookassa.model.PaymentConfirmation
import java.time.LocalDateTime

data class PaymentRequest(
    val amount: PaymentAmount,
    val capture: Boolean,
    val confirmation: PaymentConfirmation,
    @JsonProperty("created_at")
    val createdAt: LocalDateTime,
    val description: String,
    val metadata: Map<String, String>? = null
)
