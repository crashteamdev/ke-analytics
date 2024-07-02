package dev.crashteam.keanalytics.service

import dev.crashteam.keanalytics.repository.clickhouse.CHProductPositionRepository
import dev.crashteam.keanalytics.repository.clickhouse.CHProductRepository
import dev.crashteam.keanalytics.repository.clickhouse.model.ChProductPositionHistory
import dev.crashteam.keanalytics.repository.clickhouse.model.ChProductSalesReport
import mu.KotlinLogging
import org.springframework.stereotype.Service
import java.time.LocalDateTime

private val log = KotlinLogging.logger {}

@Service
class ProductServiceV2(
    private val chProductRepository: CHProductRepository,
    private val chProductPositionRepository: CHProductPositionRepository,
) {

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

    fun getProductPosition(
        categoryId: Long,
        productId: Long,
        skuId: Long,
        fromTime: LocalDateTime,
        toTime: LocalDateTime,
    ): MutableList<ChProductPositionHistory> {
        return chProductPositionRepository.getProductPositionHistory(
            categoryId.toString(),
            productId.toString(),
            skuId.toString(),
            fromTime,
            toTime,
        )
    }

}
