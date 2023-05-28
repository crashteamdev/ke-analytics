package dev.crashteam.keanalytics.domain.mongo.converter

import dev.crashteam.keanalytics.controller.converter.DataConverter
import dev.crashteam.keanalytics.domain.mongo.ProductChangeTimeSeries
import dev.crashteam.keanalytics.domain.mongo.ProductIdSkuId
import dev.crashteam.keanalytics.domain.mongo.ProductMetadata
import dev.crashteam.keanalytics.domain.mongo.ProductSkuCharacteristic
import dev.crashteam.keanalytics.stream.model.KeProductCategoryStreamRecord
import dev.crashteam.keanalytics.stream.model.KeProductItemStreamRecord
import org.springframework.stereotype.Component
import java.time.Instant

@Component
class KeProductRecordToProductChangeTimeseriesConverter :
    DataConverter<KeProductItemStreamRecord, Array<ProductChangeTimeSeries>> {

    override fun convert(source: KeProductItemStreamRecord): Array<ProductChangeTimeSeries>? {
        val productChangeTimeSeries = source.skuList.map { productSplit ->
            val characteristics = productSplit.characteristics.map {
                ProductSkuCharacteristic(it.title, it.title, it.value)
            }
            ProductChangeTimeSeries(
                rating = source.rating.toBigDecimal(),
                reviewsAmount = source.reviewsAmount,
                totalOrderAmount = source.orders,
                totalAvailableAmount = source.totalAvailableAmount,
                skuAvailableAmount = productSplit.availableAmount,
                skuCharacteristic = characteristics,
                title = source.title,
                price = productSplit.purchasePrice.toBigDecimal(),
                fullPrice = productSplit.fullPrice?.toBigDecimal(),
                photoKey = productSplit.photoKey,
                metadata = ProductMetadata(
                    id = ProductIdSkuId(
                        productId = source.productId,
                        skuId = productSplit.skuId
                    ),
                    category = source.category.title.trim(),
                    categoryPath = productCategoryToMaterializedPath(source.category),
                    sellerTitle = source.seller.sellerTitle,
                    sellerLink = source.seller.sellerLink,
                    sellerAccountId = source.seller.accountId,
                    sellerId = source.seller.id
                ),
                timestamp = Instant.now()
            )
        }

        return productChangeTimeSeries.toTypedArray()
    }

    private fun productCategoryToMaterializedPath(productCategory: KeProductCategoryStreamRecord): String {
        val categoriesTitleList = mutableListOf<String>()
        var currentCategory: KeProductCategoryStreamRecord? = productCategory
        while (currentCategory != null) {
            categoriesTitleList.add(currentCategory.title.trim())
            currentCategory = currentCategory.parent
        }
        val sb = StringBuilder()
        sb.append(",")
        for (categoryTitle in categoriesTitleList.reversed()) {
            sb.append(categoryTitle).append(",")
        }

        return sb.toString()
    }
}
