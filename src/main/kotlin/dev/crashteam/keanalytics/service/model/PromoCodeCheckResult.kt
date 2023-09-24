package dev.crashteam.keanalytics.service.model

data class PromoCodeCheckResult(
    val checkCode: PromoCodeCheckCode,
    val description: String,
)

enum class PromoCodeCheckCode {
    INVALID_USE_LIMIT,
    INVALID_DATE_LIMIT,
    NOT_FOUND,
    VALID,
}
