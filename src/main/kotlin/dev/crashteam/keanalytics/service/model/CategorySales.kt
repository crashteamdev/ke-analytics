package dev.crashteam.keanalytics.service.model

import java.math.BigDecimal

data class AggregateSalesProduct(
    val productId: Long,
    val skuId: Long,
    val name: String,
    val seller: CategorySalesSeller,
    val category: CategoryData,
    val availableAmount: Long,
    val price: BigDecimal,
    val proceeds: BigDecimal,
    val priceGraph: List<Long>,
    val orderGraph: List<Long>,
    val daysInStock: Int,
)

data class CategoryData(
    val name: String
)

data class CategorySalesSeller(
    val id: Long?,
    val name: String,
)
