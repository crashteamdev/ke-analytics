package dev.crashteam.keanalytics.controller.converter

import dev.crashteam.keanalytics.controller.model.CategoryDataView
import dev.crashteam.keanalytics.controller.model.CategorySalesSellerView
import dev.crashteam.keanalytics.controller.model.CategorySalesView
import dev.crashteam.keanalytics.controller.model.CategorySalesViewWrapper
import dev.crashteam.keanalytics.service.model.AggregateSalesWrapper
import org.springframework.stereotype.Component

@Component
class CategoryHistoryToDataConverter : DataConverter<AggregateSalesWrapper, CategorySalesViewWrapper> {

    override fun convert(source: AggregateSalesWrapper): CategorySalesViewWrapper {
        return CategorySalesViewWrapper().apply {
            data = source.data.map {
                CategorySalesView().apply {
                    productId = it.productId
                    skuId = it.skuId
                    name = it.name
                    seller = CategorySalesSellerView().apply {
                        id = it.seller.id
                        name = it.seller.name
                    }
                    category = CategoryDataView().apply {
                        name = it.category.name
                    }
                    availableAmount = it.availableAmount
                    price = it.price
                    proceeds = it.proceeds
                    priceGraph = it.priceGraph
                    orderGraph = it.orderGraph
                    daysInStock = it.daysInStock
                }
            }
            pageSize = source.meta.pageSize
            page = source.meta.page
            totalPages = source.meta.pages
        }
    }
}
