package dev.crashteam.keanalytics.converter.view

import dev.crashteam.keanalytics.controller.model.ProductSkuHistoricalCharacteristicView
import dev.crashteam.keanalytics.controller.model.ProductSkuHistoricalView
import dev.crashteam.keanalytics.converter.DataConverter
import dev.crashteam.keanalytics.service.model.ProductSkuHistorical
import org.springframework.stereotype.Component
import java.time.LocalDateTime
import java.time.ZoneOffset

@Component
class ProductSkuHistoricalToDataConverter : DataConverter<ProductSkuHistorical, ProductSkuHistoricalView> {

    override fun convert(source: ProductSkuHistorical): ProductSkuHistoricalView {
        return ProductSkuHistoricalView(
            productId = source.productId,
            skuId = source.skuId,
            name = source.name,
            orderAmount = source.orderAmount,
            reviewsAmount = source.reviewsAmount,
            totalAvailableAmount = source.totalAvailableAmount
        ).apply {
            fullPrice = source.fullPrice
            purchasePrice = source.price
            availableAmount = source.availableAmount
            salesAmount = source.salesAmount
            photoKey = source.photoKey
            characteristic = source.characteristic.map {
                ProductSkuHistoricalCharacteristicView(it.type, it.title, it.value)
            }
            date = LocalDateTime.ofInstant(source.date, ZoneOffset.UTC)
        }
    }
}
