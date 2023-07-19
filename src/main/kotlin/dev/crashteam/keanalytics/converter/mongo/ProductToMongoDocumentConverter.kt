package dev.crashteam.keanalytics.converter.mongo

import dev.crashteam.keanalytics.client.kazanexpress.model.*
import dev.crashteam.keanalytics.converter.DataConverter
import dev.crashteam.keanalytics.domain.mongo.ProductDocument
import dev.crashteam.keanalytics.domain.mongo.ProductSplitCharacteristicDocument
import dev.crashteam.keanalytics.domain.mongo.ProductSplitDocument
import dev.crashteam.keanalytics.domain.mongo.SellerDocument
import org.springframework.context.annotation.Lazy
import org.springframework.core.convert.ConversionService
import org.springframework.stereotype.Component

@Component
class ProductToMongoDocumentConverter(
    @Lazy private val conversionService: ConversionService
) : DataConverter<ProductData, ProductDocument> {

    override fun convert(source: ProductData): ProductDocument {
        return ProductDocument(
            productId = source.id,
            title = source.title,
            parentCategory = source.category.title.trim(),
            ancestorCategories = productCategoriesToAncestorCategories(source.category),
            reviewsAmount = source.reviewsAmount,
            orderAmount = source.ordersAmount,
            rOrdersAmount = source.rOrdersAmount,
            rating = source.rating,
            totalAvailableAmount = source.totalAvailableAmount,
            description = source.description,
            attributes = source.attributes,
            tags = source.tags,
            split = source.skuList?.map { productSplit ->
                productSplitToMongoDomain(productSplit, source)
            },
            seller = conversionService.convert(source.seller, SellerDocument::class.java)!!,
        )
    }

    private fun productSplitToMongoDomain(
        productSplit: ProductSplit,
        product: ProductData,
    ): ProductSplitDocument {
        // Find photo
        val photo: ProductPhoto? = productSplit.characteristics.mapNotNull {
            val productCharacteristic = product.characteristics[it.charIndex]
            val characteristicValue = productCharacteristic.values[it.valueIndex]
            val value = characteristicValue.value
            product.photos.filter { photo -> photo.color != null }.find { photo -> photo.color == value }
        }.firstOrNull() ?: product.photos.firstOrNull()
        return ProductSplitDocument(
            id = productSplit.id,
            availableAmount = productSplit.availableAmount,
            fullPrice = productSplit.fullPrice,
            purchasePrice = productSplit.purchasePrice,
            characteristics = productSplit.characteristics.map {
                val productCharacteristic = product.characteristics[it.charIndex]
                val characteristicValue = productCharacteristic.values[it.valueIndex]
                ProductSplitCharacteristicDocument(
                    productCharacteristic.title,
                    characteristicValue.title,
                    characteristicValue.value
                )
            },
            photoKey = photo?.photoKey
        )
    }

    private fun productCategoriesToAncestorCategories(productCategory: ProductCategory): List<String> {
        val ancestorCategories: MutableList<String> = ArrayList()
        var currentCategory: ProductCategory? = productCategory
        while (currentCategory != null) {
            ancestorCategories.add(currentCategory.title.trim())
            currentCategory = currentCategory.parent
        }

        return ancestorCategories
    }

}
