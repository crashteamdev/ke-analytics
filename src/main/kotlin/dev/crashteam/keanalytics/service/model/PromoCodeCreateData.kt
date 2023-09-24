package dev.crashteam.keanalytics.service.model

import dev.crashteam.keanalytics.domain.mongo.PromoCodeType
import java.time.LocalDateTime

data class PromoCodeCreateData(
    val description: String,
    val validUntil: LocalDateTime,
    val useLimit: Int,
    val type: PromoCodeType,
    val discount: Short? = null,
    val additionalDays: Int? = null,
    val prefix: String? = null,
)
