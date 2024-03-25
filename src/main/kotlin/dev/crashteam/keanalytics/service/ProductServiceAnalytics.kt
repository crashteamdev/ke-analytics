package dev.crashteam.keanalytics.service

import dev.crashteam.keanalytics.config.RedisConfig
import dev.crashteam.keanalytics.repository.clickhouse.CHCategoryRepository
import dev.crashteam.keanalytics.repository.clickhouse.CHProductRepository
import dev.crashteam.keanalytics.repository.clickhouse.model.ChCategoryOverallInfo
import dev.crashteam.keanalytics.repository.clickhouse.model.ChProductAdditionalInfo
import dev.crashteam.keanalytics.repository.clickhouse.model.ChProductSalesHistory
import dev.crashteam.keanalytics.repository.clickhouse.model.ChProductsSales
import dev.crashteam.keanalytics.service.model.ProductDailyAnalytics
import dev.crashteam.keanalytics.service.model.ProductDailyAnalyticsCategory
import dev.crashteam.keanalytics.service.model.ProductDailyAnalyticsSeller
import dev.crashteam.keanalytics.service.model.SellerOverallInfo
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import org.springframework.cache.annotation.Cacheable
import org.springframework.stereotype.Component
import java.math.RoundingMode
import java.time.LocalDate
import java.time.LocalDateTime

@Component
class ProductServiceAnalytics(
    private val chProductRepository: CHProductRepository,
    private val chCategoryRepository: CHCategoryRepository,
) {

    fun getProductAdditionalInfo(
        productId: String,
        skuId: String,
        fromTime: LocalDateTime,
        toTime: LocalDateTime
    ): ChProductAdditionalInfo? {
        return chProductRepository.getProductAdditionalInfo(productId, skuId, fromTime, toTime)
    }

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
                salesDynamic = chSellerOrderDynamics,
                productWithoutSales = chSellerOverallInfo.productCountWithoutSales
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

    fun getProductDailyAnalytics(
        productId: String,
        fromDate: LocalDate,
        toDate: LocalDate,
    ): ProductDailyAnalytics? {
        val productDailyAnalytics =
            chProductRepository.getProductDailyAnalytics(productId, fromDate, toDate) ?: return null
        val categoryTitle = chCategoryRepository.getCategoryTitle(productDailyAnalytics.categoryId) ?: "unknown"
        return ProductDailyAnalytics(
            productId = productDailyAnalytics.productId,
            title = productDailyAnalytics.title,
            category = ProductDailyAnalyticsCategory(
                categoryId = productDailyAnalytics.categoryId,
                categoryName = categoryTitle
            ),
            seller = ProductDailyAnalyticsSeller(
                sellerLink = productDailyAnalytics.sellerLink,
                sellerTitle = productDailyAnalytics.sellerTitle,
            ),
            price = productDailyAnalytics.price.setScale(2, RoundingMode.HALF_UP),
            fullPrice = productDailyAnalytics.fullPrice.setScale(2, RoundingMode.HALF_UP),
            reviewAmount = productDailyAnalytics.reviewAmount,
            revenue = productDailyAnalytics.revenue.setScale(2, RoundingMode.HALF_UP),
            photoKey = productDailyAnalytics.photoKey,
            priceChart = productDailyAnalytics.priceChart,
            revenueChart = productDailyAnalytics.revenueChart,
            orderChart = productDailyAnalytics.orderChart,
            availableChart = productDailyAnalytics.availableChart,
            firstDiscovered = productDailyAnalytics.firstDiscovered
        )
    }
}

