package dev.crashteam.keanalytics.controller.model

data class PaymentSubscriptionUpgradeCreate(
    val redirectUrl: String,
    val subscriptionType: Int
)
