package dev.crashteam.keanalytics.service.model

import dev.crashteam.keanalytics.domain.mongo.PromoCodeType

data class CallbackPaymentAdditionalInfo(
    val promoCode: String,
    val promoCodeType: PromoCodeType,
)
