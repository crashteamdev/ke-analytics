package dev.crashteam.keanalytics.calculator

import dev.crashteam.keanalytics.domain.mongo.UserSubscription

data class PriceCalculatorContext(
    val userId: String,
    val multiply: Short = 1,
    val subscription: UserSubscription,
)
