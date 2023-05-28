package dev.crashteam.keanalytics.repository.mongo.model

data class FindProductFilter(
    val category: String? = null,
    val sellerName: String? = null,
    val sellerLink: String? = null,
    val productName: String? = null,
    val orderAmountGt: Long? = null,
    val orderAmountLt: Long? = null,
)
