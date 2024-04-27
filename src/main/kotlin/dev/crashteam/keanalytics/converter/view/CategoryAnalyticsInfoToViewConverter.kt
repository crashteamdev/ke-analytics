package dev.crashteam.keanalytics.converter.view

import dev.crashteam.keanalytics.converter.DataConverter
import dev.crashteam.keanalytics.extensions.toMoney
import dev.crashteam.keanalytics.service.model.CategoryAnalyticsInfo
import dev.crashteam.mp.external.analytics.category.Category
import dev.crashteam.mp.external.analytics.category.CategoryAnalytics
import dev.crashteam.mp.external.analytics.category.CategoryAnalyticsDifference
import dev.crashteam.mp.external.analytics.category.CategoryChildrenId
import org.springframework.stereotype.Component

@Component
class CategoryAnalyticsInfoToViewConverter :
    DataConverter<CategoryAnalyticsInfo, dev.crashteam.mp.external.analytics.category.CategoryAnalyticsInfo> {

    override fun convert(source: CategoryAnalyticsInfo): dev.crashteam.mp.external.analytics.category.CategoryAnalyticsInfo? {
        return dev.crashteam.mp.external.analytics.category.CategoryAnalyticsInfo.newBuilder().apply {
            this.category = Category.newBuilder().apply {
                this.categoryId = source.category.categoryId
                this.name = source.category.name
                source.category.parentId?.let { this.parentId = it }
                this.addAllChildrenIds(source.category.childrenIds.map { childrenCategoryId ->
                    CategoryChildrenId.newBuilder().apply {
                        this.childrenId = childrenCategoryId
                    }.build()
                })
            }.build()
            this.categoryAnalytics = CategoryAnalytics.newBuilder().apply {
                this.revenue = source.analytics.revenue.toMoney()
                this.revenuePerProduct = source.analytics.revenuePerProduct.toMoney()
                this.salesCount = source.analytics.salesCount
                this.productCount = source.analytics.productCount
                this.averageBill = source.analytics.averageBill.toMoney()
                this.sellerCount = source.analytics.sellerCount
                this.tsts = source.analytics.tsts.toDouble()
                this.tstc = source.analytics.tstc.toDouble()
            }.build()
            this.previousPeriodAnalytics = CategoryAnalytics.newBuilder().apply {
                this.revenue = source.analyticsPrevPeriod.revenue.toMoney()
                this.revenuePerProduct = source.analyticsPrevPeriod.revenuePerProduct.toMoney()
                this.salesCount = source.analyticsPrevPeriod.salesCount
                this.productCount = source.analyticsPrevPeriod.productCount
                this.averageBill = source.analyticsPrevPeriod.averageBill.toMoney()
                this.sellerCount = source.analyticsPrevPeriod.sellerCount
                this.tsts = source.analyticsPrevPeriod.tsts.toDouble()
                this.tstc = source.analyticsPrevPeriod.tstc.toDouble()
            }.build()
            this.categoryAnalyticsDifference = CategoryAnalyticsDifference.newBuilder().apply {
                this.revenuePercentage = source.analyticsDifference.revenuePercentage.toDouble()
                this.revenuePerProductPercentage = source.analyticsDifference.revenuePerProductPercentage.toDouble()
                this.salesCountPercentage = source.analyticsDifference.salesCountPercentage.toDouble()
                this.productCountPercentage = source.analyticsDifference.productCountPercentage.toDouble()
                this.averageBillPercentage = source.analyticsDifference.averageBillPercentage.toDouble()
                this.sellerCountPercentage = source.analyticsDifference.sellerCountPercentage.toDouble()
                this.tstsPercentage = source.analyticsDifference.tstsPercentage.toDouble()
                this.tstcPercentage = source.analyticsDifference.tstcPercentage.toDouble()
            }.build()
        }.build()
    }
}
