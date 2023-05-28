package dev.crashteam.keanalytics.domain.mongo

import org.springframework.data.mongodb.core.mapping.TimeSeries
import org.springframework.data.mongodb.core.timeseries.Granularity
import java.math.BigDecimal
import java.time.Instant

@TimeSeries(
    collection = "product_change",
    timeField = "timestamp",
    metaField = "metadata",
    granularity = Granularity.HOURS
)
data class ProductChangeTimeSeries(
    val rating: BigDecimal?,
    val reviewsAmount: Long,
    val totalOrderAmount: Long,
    val totalAvailableAmount: Long,
    val skuAvailableAmount: Long,
    val skuCharacteristic: List<ProductSkuCharacteristic>? = null,
    val title: String,
    val price: BigDecimal,
    val fullPrice: BigDecimal? = null,
    val photoKey: String? = null,
    val metadata: ProductMetadata,
    val timestamp: Instant
)

data class ProductMetadata(
    val id: ProductIdSkuId,
    val category: String?,
    val categoryPath: String?,
    val sellerTitle: String,
    val sellerLink: String,
    val sellerAccountId: Long?,
    val sellerId: Long?,
)

data class ProductIdSkuId(
    val productId: Long,
    val skuId: Long,
)

