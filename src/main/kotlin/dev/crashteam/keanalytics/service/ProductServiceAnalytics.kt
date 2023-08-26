package dev.crashteam.keanalytics.service

import dev.crashteam.keanalytics.config.RedisConfig
import dev.crashteam.keanalytics.repository.clickhouse.CHProductRepository
import dev.crashteam.keanalytics.repository.clickhouse.model.ChCategoryOverallInfo
import dev.crashteam.keanalytics.repository.clickhouse.model.ChProductSalesHistory
import dev.crashteam.keanalytics.repository.clickhouse.model.ChProductsSales
import dev.crashteam.keanalytics.service.model.SellerOverallInfo
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import org.springframework.cache.annotation.Cacheable
import org.springframework.stereotype.Component
import java.time.LocalDateTime

@Component
class ProductServiceAnalytics(
    private val chProductRepository: CHProductRepository
) {

    @Cacheable(value = [RedisConfig.CATEGORY_OVERALL_INFO_CACHE], unless = "#result == null")
    fun getCategoryOverallAnalytics(
        categoryId: Long,
        fromTime: LocalDateTime,
        toTime: LocalDateTime
    ): ChCategoryOverallInfo? {
        return chProductRepository.getCategoryAnalytics(categoryId, fromTime, toTime)
    }

    @ExperimentalCoroutinesApi
    @Cacheable(value = [RedisConfig.SELLER_OVERALL_INFO_CACHE_NAME], unless = "#result == null")
    fun getSellerAnalytics(
        sellerLink: String,
        fromTime: LocalDateTime,
        toTime: LocalDateTime,
    ): SellerOverallInfo? {
        val sellerOverallInfo = runBlocking {
            val sellerAnalytics = async {
                chProductRepository.getSellerAnalytics(sellerLink, fromTime, toTime)
            }
            val sellerOrderDynamic = async {
                chProductRepository.getSellerOrderDynamic(sellerLink, fromTime, toTime)
            }
            awaitAll(sellerAnalytics, sellerOrderDynamic)
            val chSellerOverallInfo = sellerAnalytics.getCompleted() ?: return@runBlocking null
            val chSellerOrderDynamics = sellerOrderDynamic.getCompleted() ?: return@runBlocking null

            return@runBlocking SellerOverallInfo(
                averagePrice = chSellerOverallInfo.averagePrice,
                revenue = chSellerOverallInfo.revenue,
                orderCount = chSellerOverallInfo.orderCount,
                productCount = chSellerOverallInfo.productCount,
                productCountWithSales = chSellerOverallInfo.productCountWithSales,
                salesDynamic = chSellerOrderDynamics
            )
        }
        return sellerOverallInfo
    }


    fun getProductAnalytics(
        productId: Long,
        skuId: Long,
        fromTime: LocalDateTime,
        toTime: LocalDateTime
    ): List<ChProductSalesHistory> {
        return chProductRepository.getProductSales(productId.toString(), skuId.toString(), fromTime, toTime)
    }

    fun getProductSalesAnalytics(
        productIds: List<Long>,
        fromTime: LocalDateTime,
        toTime: LocalDateTime
    ): List<ChProductsSales> {
        val productIdList = productIds.map { it.toString() }
        return chProductRepository.getProductsSales(productIdList, fromTime, toTime)
    }
}

