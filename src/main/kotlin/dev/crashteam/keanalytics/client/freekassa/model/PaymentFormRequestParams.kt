package dev.crashteam.keanalytics.client.freekassa.model

import java.math.BigDecimal

data class PaymentFormRequestParams(
    val userId: String,
    val orderId: String,
    val email: String,
    val amount: BigDecimal,
    val currency: String,
    val subscriptionId: Int,
    val referralCode: String? = null,
    val multiply: Short = 1
)
