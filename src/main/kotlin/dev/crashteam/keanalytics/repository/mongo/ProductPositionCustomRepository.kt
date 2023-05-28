package dev.crashteam.keanalytics.repository.mongo

import dev.crashteam.keanalytics.repository.mongo.model.ProductPositionAggregate
import reactor.core.publisher.Flux
import java.time.LocalDateTime

interface ProductPositionCustomRepository {

    fun findProductPositions(
        categoryId: Long,
        productId: Long,
        skuId: Long,
        fromTime: LocalDateTime,
        toTime: LocalDateTime,
    ): Flux<ProductPositionAggregate>

}
