package dev.crashteam.keanalytics.controller.model

import com.fasterxml.jackson.annotation.JsonFormat
import java.time.LocalDate

data class ProductPositionView(
    val categoryId: Long,
    val productId: Long,
    val skuId: Long,
    val history: List<ProductPositionHistoryView>
)

data class ProductPositionHistoryView(
    val position: Long,
    @JsonFormat(pattern = "dd.MM.yyyy")
    val date: LocalDate
)
