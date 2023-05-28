package dev.crashteam.keanalytics.repository.mongo

import dev.crashteam.keanalytics.repository.mongo.model.*
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.LocalDateTime

interface ProductChangeTimeSeriesCustomRepository {
    fun findProductHistoryBySkuId(
        productId: Long,
        skuId: Long,
        fromTime: LocalDateTime,
        toTime: LocalDateTime,
        limit: Int,
        offset: Int,
    ): Mono<ProductHistorySkuAggregateV2>

    fun findProductHistoryByProductIds(
        productIds: List<Long>,
        fromTime: LocalDateTime,
        toTime: LocalDateTime,
    ): Flux<MultipleProductHistorySalesV2>

    fun findProductHistoryBySellers(
        sellerLink: String,
        fromTime: LocalDateTime,
        toTime: LocalDateTime,
        limit: Int,
        offset: Int,
    ): Mono<ProductSellerHistoryAggregateWrapper>

    fun findProductHistoryByCategory(
        categoryPath: List<String>,
        fromTime: LocalDateTime,
        toTime: LocalDateTime,
        limit: Int,
        offset: Int,
    ): Mono<MutableList<ProductSellerHistoryAggregate>>
}
