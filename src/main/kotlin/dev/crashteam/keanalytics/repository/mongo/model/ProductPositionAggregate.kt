package dev.crashteam.keanalytics.repository.mongo.model

import java.time.LocalDate

class ProductPositionAggregate {
    var id: ProductPositionAggregateId? = null
    var position: Long? = null
}

data class ProductPositionAggregateId(
    val categoryId: Long,
    val productId: Long,
    val skuId: Long,
    val date: LocalDate? = null
)

