package dev.crashteam.keanalytics.controller.model

import com.fasterxml.jackson.annotation.JsonFormat
import java.time.LocalDateTime

data class UserSubscriptionView(
    val active: Boolean,
    @JsonFormat(pattern = "dd.MM.yyyy HH:mm:ss")
    val createdAt: LocalDateTime,
    @JsonFormat(pattern = "dd.MM.yyyy HH:mm:ss")
    val endAt: LocalDateTime,
    val type: String?,
    val typeNumeric: Int
)
