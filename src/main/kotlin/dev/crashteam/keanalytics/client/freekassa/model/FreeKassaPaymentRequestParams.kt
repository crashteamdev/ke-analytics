package dev.crashteam.keanalytics.client.freekassa.model

import java.math.BigDecimal

data class FreeKassaPaymentRequestParams(
    val orderId: Int,
    val paymentId: String,
    val paymentSystemIdentifier: Int,
    val email: String,
    val ip: String,
    val amount: BigDecimal,
    val currency: String,
)
