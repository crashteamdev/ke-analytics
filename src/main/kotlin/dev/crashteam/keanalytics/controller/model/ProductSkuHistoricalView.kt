package dev.crashteam.keanalytics.controller.model

import com.fasterxml.jackson.annotation.JsonFormat
import java.math.BigDecimal
import java.time.LocalDateTime

class ProductSkuHistoricalView(
    val productId: Long,
    val skuId: Long,
    val name: String,
    val orderAmount: Long,
    val reviewsAmount: Long,
    val totalAvailableAmount: Long,
) {
    var fullPrice: BigDecimal? = null
    var purchasePrice: BigDecimal? = null
    var availableAmount: Long? = null
    var salesAmount: BigDecimal? = null
    var photoKey: String? = null
    var characteristic: List<ProductSkuHistoricalCharacteristicView> = emptyList()
    @JsonFormat(pattern = "dd.MM.yyyy")
    var date: LocalDateTime? = null
}

data class ProductSkuHistoricalCharacteristicView(
    val type: String,
    val title: String,
    val value: String
)
