package dev.crashteam.keanalytics.repository.mongo.model

data class ProductTotalOrderAggregate(
    val productId: String,
    val title: String,
    val productOrderAmount: Long,
    val seller: String
)

data class ProductCategoryTotalOrderAggregate(
    val title: String,
    val parent: ProductCategoryTotalOrderAggregate?
)

data class ProductSellerTotalOrderAggregate(
    val title: String
)
