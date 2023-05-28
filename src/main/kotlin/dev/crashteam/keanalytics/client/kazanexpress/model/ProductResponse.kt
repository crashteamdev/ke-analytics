package dev.crashteam.keanalytics.client.kazanexpress.model

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty
import java.math.BigDecimal

@JsonIgnoreProperties(ignoreUnknown = true)
data class ProductResponse(
    val payload: ProductDataWrapper?,
)

data class ProductDataWrapper(
    val data: ProductData,
)

data class ProductData(
    val id: Long,
    val title: String,
    val category: ProductCategory,
    val reviewsAmount: Long,
    val ordersAmount: Long,
    @JsonProperty("rordersAmount")
    val rOrdersAmount: Long,
    val rating: BigDecimal,
    val totalAvailableAmount: Long,
    val charityCommission: Long,
    val description: String,
    val attributes: List<String>,
    val tags: List<String>,
    val photos: List<ProductPhoto>,
    val characteristics: List<ProductCharacteristic>,
    val skuList: List<ProductSplit>?,
    val seller: Seller,
)

data class ProductPhoto(
    val photo: Map<String, ProductPhotoQuality>,
    val color: String?,
    val photoKey: String,
)

data class ProductPhotoQuality(
    val high: String,
    val low: String,
)

data class ProductCategory(
    val id: Long,
    val title: String,
    val productAmount: Long,
    val parent: ProductCategory?,
)

data class ProductSplit(
    val id: Long,
    val characteristics: List<ProductSplitCharacteristic>,
    val availableAmount: Long,
    val fullPrice: BigDecimal?,
    val purchasePrice: BigDecimal,
)

data class ProductSplitCharacteristic(
    val charIndex: Int,
    val valueIndex: Int,
)

data class Seller(
    val id: Long,
    val title: String,
    val link: String,
    val description: String?,
    val rating: BigDecimal,
    val sellerAccountId: Long,
    val isEco: Boolean,
    val adultCategory: Boolean,
    val contacts: List<Contact>,
)

data class ProductCharacteristic(
    val title: String,
    val values: List<CharacteristicValue>,
)

data class CharacteristicValue(
    val id: Long,
    val title: String,
    val value: String,
)

data class Contact(
    val type: String,
    val value: String,
)
