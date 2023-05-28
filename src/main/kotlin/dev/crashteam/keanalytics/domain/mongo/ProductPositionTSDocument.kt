package dev.crashteam.keanalytics.domain.mongo

import org.springframework.data.mongodb.core.mapping.TimeSeries
import org.springframework.data.mongodb.core.timeseries.Granularity
import java.time.Instant

@TimeSeries(
    collection = "product_positions",
    timeField = "timestamp",
    metaField = "metadata",
    granularity = Granularity.HOURS
)
data class ProductPositionTSDocument(
    val position: Long,
    val metadata: ProductPositionMetadata,
    val timestamp: Instant
)

data class ProductPositionMetadata(
    val id: ProductPositionId,
    val categoryId: Long,
)

data class ProductPositionId(
    val productId: Long,
    val skuId: Long,
)

