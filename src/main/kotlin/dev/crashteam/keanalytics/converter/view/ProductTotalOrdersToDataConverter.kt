package dev.crashteam.keanalytics.converter.view

import dev.crashteam.keanalytics.controller.model.ProductTotalOrderSellerView
import dev.crashteam.keanalytics.controller.model.ProductTotalOrdersView
import dev.crashteam.keanalytics.converter.DataConverter
import dev.crashteam.keanalytics.repository.mongo.model.ProductTotalOrdersAggregate
import org.springframework.stereotype.Component
import java.math.RoundingMode

@Component
class ProductTotalOrdersToDataConverter : DataConverter<ProductTotalOrdersAggregate, ProductTotalOrdersView> {

    override fun convert(source: ProductTotalOrdersAggregate): ProductTotalOrdersView? {
        return ProductTotalOrdersView(
            totalOrderAmount = source.totalOrderAmount,
            earnings = source.earnings.setScale(2, RoundingMode.HALF_UP),
            quantity = source.quantity,
            dailyOrder = source.dailyOrder.setScale(2, RoundingMode.HALF_UP),
            seller = ProductTotalOrderSellerView(
                source.seller.title,
                source.seller.link,
                source.seller.accountId ?: source.seller.sellerAccountId
            ),
        )
    }
}
