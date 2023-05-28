package dev.crashteam.keanalytics.controller.model

import com.fasterxml.jackson.annotation.JsonProperty
import java.math.BigDecimal

data class ProductView(
    @JsonProperty("product_id")
    val productId: Long,
    val title: String,
    @JsonProperty("parent_category")
    val parentCategory: String?,
    @JsonProperty("ancestor_categories")
    val ancestorCategories: List<String>?,
    @JsonProperty("reviewsAmount")
    val reviewsAmount: Long,
    @JsonProperty("order_amount")
    val orderAmount: Long,
    @JsonProperty("r_orders_amount")
    val rOrdersAmount: Long,
    @JsonProperty("total_available_amount")
    val totalAvailableAmount: Long,
    val description: String,
    val attributes: List<String>,
    val tags: List<String>,
    val seller: ProductSellerView,
    val items: List<ProductItemView>?,
)

data class ProductSellerView(
    val id: Long,
    val title: String,
    val link: String,
    val description: String?,
    val rating: BigDecimal,
    @JsonProperty("seller_account_id")
    val sellerAccountId: Long,
    @JsonProperty("is_eco")
    val isEco: Boolean,
    @JsonProperty("adult_category")
    val adultCategory: Boolean,
)

data class ProductItemView(
    val id: Long,
    val characteristics: List<ItemCharacteristicView>,
    @JsonProperty("available_amount")
    val availableAmount: Long,
    @JsonProperty("full_price")
    val fullPrice: BigDecimal?,
    @JsonProperty("purchase_price")
    val purchasePrice: BigDecimal,
    val photoKey: String?,
)

data class ItemCharacteristicView(
    val type: String,
    val title: String,
    val value: String,
)
