package dev.crashteam.keanalytics.service.model

import java.math.BigDecimal

data class CategoryAnalyticsInfo(
    val category: Category,
    val analytics: CategoryAnalytics,
    val analyticsPrevPeriod: CategoryAnalytics,
    val analyticsDifference: CategoryAnalyticsDifference
)

data class Category(
    val categoryId: Long,
    val name: String,
    val parentId: Long? = null,
    val childrenIds: List<Long>,
)

data class CategoryAnalytics(
    val revenue: BigDecimal,
    val revenuePerProduct: BigDecimal,
    val salesCount: Long,
    val productCount: Long,
    val sellerCount: Long,
    val averageBill: BigDecimal,
    val tsts: BigDecimal,
    val tstc: BigDecimal,
)

data class CategoryAnalyticsDifference(
    val revenuePercentage: BigDecimal,
    val revenuePerProductPercentage: BigDecimal,
    val salesCountPercentage: BigDecimal,
    val productCountPercentage: BigDecimal,
    val sellerCountPercentage: BigDecimal,
    val averageBillPercentage: BigDecimal,
    val tstsPercentage: BigDecimal,
    val tstcPercentage: BigDecimal,
)
