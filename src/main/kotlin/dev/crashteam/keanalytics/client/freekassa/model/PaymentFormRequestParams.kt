package dev.crashteam.keanalytics.client.freekassa.model

import dev.crashteam.keanalytics.domain.mongo.PromoCodeType
import java.math.BigDecimal

data class PaymentFormRequestParams(
    val userId: String,
    val orderId: String,
    val email: String,
    val amount: BigDecimal,
    val currency: String,
    val subscriptionId: Int,
    val referralCode: String? = null,
    val promoCode: String? = null,
    val promoCodeType: PromoCodeType? = null,
    val multiply: Short = 1
)
