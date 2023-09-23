package dev.crashteam.keanalytics.calculator

data class PriceCalculatorOption(
    val promoCode: String? = null,
    val referralCode: String? = null,
    val context: PriceCalculatorContext,
)
