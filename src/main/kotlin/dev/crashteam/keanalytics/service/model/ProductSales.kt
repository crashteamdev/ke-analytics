package dev.crashteam.keanalytics.service.model

import java.math.BigDecimal
import java.time.Instant
import java.time.LocalDateTime
import java.util.Date

data class ProductSales(
    val productId: Long,
    val skuId: Long,
    val seller: ProductSalesSeller,
    val dayChange: List<ProductDayChange>
)

data class ProductSalesSeller(
    val title: String,
    val link: String,
    val accountId: Long?
)

data class ProductDayChange(
    var date: Instant,
    val orderAmount: Long,
    val reviewsAmount: Long,
    var price: BigDecimal,
    var availableAmount: Long,
    var salesAmount: BigDecimal,
)
