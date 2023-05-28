package dev.crashteam.keanalytics.controller.model

data class PaymentCreateResponse(
    val paymentId: String,
    val status: String,
    val paid: Boolean,
    val amount: String,
    val currency: String,
    val confirmationUrl: String
)
