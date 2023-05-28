package dev.crashteam.keanalytics.service

import mu.KotlinLogging
import dev.crashteam.keanalytics.repository.mongo.ProductChangeTimeSeriesRepository
import dev.crashteam.keanalytics.repository.mongo.model.*
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.LocalDateTime
import java.time.OffsetDateTime

private val log = KotlinLogging.logger {}

@Service
class ProductServiceV2(
    private val productChangeTimeSeriesRepository: ProductChangeTimeSeriesRepository
) {

    fun getProductSkuSalesHistory(
        productId: Long,
        skuId: Long,
        fromTime: OffsetDateTime,
        toTime: OffsetDateTime,
        limit: Int,
        offset: Int,
    ): Mono<ProductHistorySkuAggregateV2> {
        log.info { "Get product sales history by productId=$productId; skuId=$skuId; fromTime=$fromTime; toTime=$toTime" }
        return productChangeTimeSeriesRepository.findProductHistoryBySkuId(
            productId,
            skuId,
            fromTime.toLocalDateTime(),
            toTime.toLocalDateTime(),
            limit,
            offset,
        )
    }

    fun getProductsSales(
        productIds: LongArray,
        fromTime: OffsetDateTime,
        toTime: OffsetDateTime,
    ): Flux<MultipleProductHistorySalesV2> {
        val productIdsList = productIds.toTypedArray().toList()
        log.info { "Get product sales by productIds=${productIdsList.joinToString(",")}; fromTime=$fromTime; toTime=$toTime" }
        return productChangeTimeSeriesRepository.findProductHistoryByProductIds(
            productIdsList,
            fromTime.toLocalDateTime(),
            toTime.toLocalDateTime()
        )
    }

    fun getSellerSales(
        link: String,
        fromTime: LocalDateTime,
        toTime: LocalDateTime,
        limit: Int,
        offset: Int,
    ): Mono<ProductSellerHistoryAggregateWrapper> {
        log.info { "Get seller sales by link=$link; fromTime=$fromTime; toTime=$toTime" }
        return productChangeTimeSeriesRepository.findProductHistoryBySellers(link, fromTime, toTime, limit, offset)
    }

    fun getCategorySales(
        categoryPath: List<String>,
        fromTime: LocalDateTime,
        toTime: LocalDateTime,
        limit: Int,
        offset: Int,
    ): Mono<MutableList<ProductSellerHistoryAggregate>> {
        log.info { "Get category sales by categoryPath=${categoryPath.joinToString(",")}; fromTime=$fromTime; toTime=$toTime" }
        return productChangeTimeSeriesRepository.findProductHistoryByCategory(
            categoryPath,
            fromTime,
            toTime,
            limit,
            offset
        )
    }
}
