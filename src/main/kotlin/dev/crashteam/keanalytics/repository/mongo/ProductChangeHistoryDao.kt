package dev.crashteam.keanalytics.repository.mongo

import dev.crashteam.keanalytics.domain.mongo.ProductChangeHistoryDocument
import org.springframework.data.mongodb.core.ReactiveMongoTemplate
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.*
import java.util.Date

@Component
class ProductChangeHistoryDao(
    private val reactiveMongoTemplate: ReactiveMongoTemplate,
) {

    fun findByProductIdAndSkuId(productId: Long, skuId: Long): Mono<ProductChangeHistoryDocument> {
        val query = Query(Criteria.where("id.productId").`is`(productId).and("id.skuId").`is`(skuId))
        return reactiveMongoTemplate.findOne(query, ProductChangeHistoryDocument::class.java)
    }

    fun findByProductId(productId: Long): Flux<ProductChangeHistoryDocument> {
        val query = Query(Criteria.where("id.productId").`is`(productId))
        return reactiveMongoTemplate.find(query, ProductChangeHistoryDocument::class.java)
    }

    fun existsByProductIdSkuIdAndDay(productId: Long, skuId: Long, day: LocalDate): Mono<Boolean> {
        val startOfDay: Instant = LocalDate.now(ZoneId.of("UTC")).atTime(LocalTime.MIN).toInstant(ZoneOffset.UTC)
        val endOfDay: Instant = LocalDate.now(ZoneId.of("UTC")).atTime(LocalTime.MAX).toInstant(ZoneOffset.UTC)
        val query = Query(
            Criteria.where("id.productId").`is`(productId).and("id.skuId").`is`(skuId)
                .andOperator(
                    Criteria.where("skuChange.date").gte(startOfDay), Criteria.where("skuChange.date").lt(endOfDay)
                )
        )
        return reactiveMongoTemplate.exists(query, ProductChangeHistoryDocument::class.java)
    }
}
