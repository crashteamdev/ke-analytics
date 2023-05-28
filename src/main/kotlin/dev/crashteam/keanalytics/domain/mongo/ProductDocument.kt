package dev.crashteam.keanalytics.domain.mongo

import org.bson.types.ObjectId
import org.springframework.data.mongodb.core.index.Indexed
import org.springframework.data.mongodb.core.mapping.Document
import org.springframework.data.mongodb.core.mapping.MongoId
import org.springframework.data.mongodb.core.mapping.TextScore
import java.math.BigDecimal
import java.time.LocalDateTime

@Document("products")
data class ProductDocument(

    @Indexed(unique = true)
    val productId: Long,

    val title: String,

    val parentCategory: String?,

    val ancestorCategories: List<String>?,

    val reviewsAmount: Long,

    val orderAmount: Long,

    val rOrdersAmount: Long? = null,

    val rating: BigDecimal?,

    val totalAvailableAmount: Long,

    val description: String,

    val attributes: List<String>,

    val tags: List<String>,

    val seller: SellerDocument,

    val split: List<ProductSplitDocument>?,

    val createdAt: LocalDateTime? = null,

    @TextScore
    val score: Float? = null,

    @MongoId
    val id: ObjectId = ObjectId(),
)

data class SellerDocument(
    val id: Long,
    val title: String,
    val link: String,
    val description: String?,
    val rating: BigDecimal,
    val sellerAccountId: Long,
    val isEco: Boolean? = null,
    val adultCategory: Boolean? = null,
    val contacts: List<ProductContactDocument>
)

data class ProductContactDocument(
    val type: String,
    val value: String
)

data class ProductSplitDocument(
    val id: Long,
    val characteristics: List<ProductSplitCharacteristicDocument>,
    val availableAmount: Long,
    val fullPrice: BigDecimal?,
    val purchasePrice: BigDecimal,
    val photoKey: String?,
)

data class ProductSplitCharacteristicDocument(
    val type: String,
    val title: String,
    val value: String,
)
