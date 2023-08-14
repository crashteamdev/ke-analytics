package dev.crashteam.keanalytics.client.freekassa.model

data class PaymentResponse(
    val type: String,
    val orderId: String,
    val orderHash: String,
    val location: String
)
