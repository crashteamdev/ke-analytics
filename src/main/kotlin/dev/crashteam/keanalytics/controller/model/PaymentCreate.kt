package dev.crashteam.keanalytics.controller.model

data class PaymentCreate(
    val amount: String,
    val currency: String,
    val redirectUrl: String,
    val description: String,
    val subscriptionType: Int,
    val multiply: Short? = null,
    val referralCode: String? = null,
)
