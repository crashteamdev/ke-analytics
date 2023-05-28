package dev.crashteam.keanalytics.repository.mongo.model

import java.math.BigDecimal

data class ProductCategoryHistoryAggregateWrapper(
    val data: List<ProductSellerHistoryAggregate>,
    val total: Long = 0,
)

data class ProductCategoryHistoryAggregate(
    val id: ProductSellerHistoryAggregateId,
    val name: String,
    val sellerId: String,
    val sellerTitle: String,
    val categoryName: String,
    val orderAmountGraph: List<ProductSellerHistoryOrderGraph>,
    val availableAmountGraph: List<ProductSellerHistoryAvailableAmountGraph>,
    val availableAmount: Long,
    val priceGraph: List<ProductSellerHistoryPriceGraph>,
    val price: BigDecimal,
    val proceeds: BigDecimal,
)

data class ProductCategoryHistoryAggregateId(
    val productId: Long,
    val skuId: Long,
)

data class ProductCategoryHistoryOrderGraph(
    val orderAmount: Long,
    val time: String,
)

data class ProductCategoryHistoryAvailableAmountGraph(
    val availableAmount: Long,
    val time: String,
)

data class ProductCategoryHistoryPriceGraph(
    val price: BigDecimal,
    val time: String
)
