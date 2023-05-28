package dev.crashteam.keanalytics.repository.mongo.model

import java.math.BigDecimal

data class ProductSellerHistoryAggregateWrapper(
    val data: List<ProductSellerHistoryAggregate>,
    val total: Long,
)

data class ProductSellerHistoryAggregate(
    val id: ProductSellerHistoryAggregateId,
    val name: String,
    val sellerId: String?,
    val sellerTitle: String,
    val categoryName: String,
    val orderAmountGraph: List<ProductSellerHistoryOrderGraph>,
    val availableAmountGraph: List<ProductSellerHistoryAvailableAmountGraph>,
    val availableAmount: Long,
    val priceGraph: List<ProductSellerHistoryPriceGraph>,
    val price: BigDecimal,
    val proceeds: BigDecimal,
)

data class ProductSellerHistoryAggregateId(
    val productId: Long,
    val skuId: Long,
)

data class ProductSellerHistoryOrderGraph(
    val orderAmount: Long,
    val time: String,
)

data class ProductSellerHistoryAvailableAmountGraph(
    val availableAmount: Long,
    val time: String,
)

data class ProductSellerHistoryPriceGraph(
    val price: BigDecimal,
    val time: String
)
