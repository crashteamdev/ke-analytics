package dev.crashteam.keanalytics.service

import dev.crashteam.keanalytics.repository.clickhouse.CHProductRepository
import dev.crashteam.keanalytics.repository.clickhouse.model.ChProductSalesReport
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
    private val productChangeTimeSeriesRepository: ProductChangeTimeSeriesRepository,
    private val chProductRepository: CHProductRepository,
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
    ): List<ChProductSalesReport> {
        log.info { "Get seller sales by link=$link; fromTime=$fromTime; toTime=$toTime; limit=$limit; offset=$offset" }
        return chProductRepository.getSellerSalesForReport(
            sellerLink = link,
            fromTime = fromTime,
            toTime = toTime,
            limit = limit,
            offset = offset,
        )
    }

    fun getCategorySales(
        categoryId: Long,
        fromTime: LocalDateTime,
        toTime: LocalDateTime,
        limit: Int,
        offset: Int,
    ): List<ChProductSalesReport> {
        log.info { "Get category sales by categoryId=$categoryId;" +
                " fromTime=$fromTime; toTime=$toTime; limit=$limit' offset=$offset" }
        return chProductRepository.getCategorySalesForReport(
            categoryId,
            fromTime,
            toTime,
            limit,
            offset
        )
    }
}
