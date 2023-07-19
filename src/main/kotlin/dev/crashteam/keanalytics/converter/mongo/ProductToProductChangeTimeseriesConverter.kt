package dev.crashteam.keanalytics.converter.mongo

import dev.crashteam.keanalytics.client.kazanexpress.model.ProductCategory
import dev.crashteam.keanalytics.client.kazanexpress.model.ProductData
import dev.crashteam.keanalytics.client.kazanexpress.model.ProductPhoto
import dev.crashteam.keanalytics.converter.DataConverter
import dev.crashteam.keanalytics.domain.mongo.ProductChangeTimeSeries
import dev.crashteam.keanalytics.domain.mongo.ProductIdSkuId
import dev.crashteam.keanalytics.domain.mongo.ProductMetadata
import dev.crashteam.keanalytics.domain.mongo.ProductSkuCharacteristic
import org.springframework.stereotype.Component
import java.time.Instant

@Component
class ProductToProductChangeTimeseriesConverter : DataConverter<ProductData, Array<ProductChangeTimeSeries>> {

    override fun convert(source: ProductData): Array<ProductChangeTimeSeries>? {
        val productChangeTimeSeries = source.skuList?.map { productSplit ->
            val characteristics = productSplit.characteristics.map {
                val productCharacteristic = source.characteristics[it.charIndex]
                val characteristicValue = productCharacteristic.values[it.valueIndex]
                ProductSkuCharacteristic(
                    productCharacteristic.title,
                    characteristicValue.title,
                    characteristicValue.value
                )
            }
            val photo: ProductPhoto? = productSplit.characteristics.mapNotNull {
                val productCharacteristic = source.characteristics[it.charIndex]
                val characteristicValue = productCharacteristic.values[it.valueIndex]
                val value = characteristicValue.value
                source.photos.filter { photo -> photo.color != null }.find { photo -> photo.color == value }
            }.firstOrNull() ?: source.photos.firstOrNull()

            ProductChangeTimeSeries(
                rating = source.rating,
                reviewsAmount = source.reviewsAmount,
                totalOrderAmount = source.ordersAmount,
                totalAvailableAmount = source.totalAvailableAmount,
                skuAvailableAmount = productSplit.availableAmount,
                skuCharacteristic = characteristics,
                title = source.title,
                price = productSplit.purchasePrice,
                fullPrice = productSplit.fullPrice,
                photoKey = photo?.photoKey,
                metadata = ProductMetadata(
                    id = ProductIdSkuId(
                        productId = source.id,
                        skuId = productSplit.id
                    ),
                    category = source.category.title.trim(),
                    categoryPath = productCategoryToMaterializedPath(source.category),
                    sellerTitle = source.seller.title,
                    sellerLink = source.seller.link,
                    sellerAccountId = source.seller.sellerAccountId,
                    sellerId = source.seller.id
                ),
                timestamp = Instant.now()
            )
        }

        return productChangeTimeSeries!!.toTypedArray()
    }

    private fun productCategoryToMaterializedPath(productCategory: ProductCategory): String {
        val categoriesTitleList = mutableListOf<String>()
        var currentCategory: ProductCategory? = productCategory
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
