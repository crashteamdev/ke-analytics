package dev.crashteam.keanalytics.converter.view

import dev.crashteam.keanalytics.converter.DataConverter
import dev.crashteam.keanalytics.extensions.toMoney
import dev.crashteam.keanalytics.extensions.toProtobufDate
import dev.crashteam.keanalytics.service.model.CategoryDailyAnalytics
import dev.crashteam.mp.external.analytics.category.CategoryDailyAnalyticsInfo
import org.springframework.stereotype.Component

@Component
class CategoryDailyAnalyticsInfoToViewConverter : DataConverter<CategoryDailyAnalytics, CategoryDailyAnalyticsInfo> {

    override fun convert(source: CategoryDailyAnalytics): CategoryDailyAnalyticsInfo {
        return CategoryDailyAnalyticsInfo.newBuilder().apply {
            this.date = source.date.toProtobufDate()
            this.revenue = source.revenue.toMoney()
            this.averageBill = source.averageBill.toMoney()
            this.salesCount = source.salesCount
            this.availableCount = source.availableCount
        }.build()
    }
}
