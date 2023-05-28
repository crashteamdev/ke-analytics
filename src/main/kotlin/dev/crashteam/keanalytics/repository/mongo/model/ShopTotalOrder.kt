package dev.crashteam.keanalytics.repository.mongo.model

data class ShopTotalOrder(
    val seller: ShopTotalSeller,
    val shopOrderAmount: Long
)

data class ShopTotalSeller(
    val link: String,
    val title: String,
)
