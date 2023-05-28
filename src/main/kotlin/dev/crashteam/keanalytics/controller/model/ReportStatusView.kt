package dev.crashteam.keanalytics.controller.model

import com.fasterxml.jackson.annotation.JsonFormat
import java.time.LocalDateTime

data class ReportStatusView(
    val reportId: String?,
    val jobId: String,
    val status: String,
    val interval: Int,
    val reportType: String,
    @JsonFormat(pattern = "yyyy-MM-dd'T'hh:mm:ss.SSS'Z'")
    val createdAt: LocalDateTime,
    val sellerLink: String?,
    val categoryId: Long?
)
