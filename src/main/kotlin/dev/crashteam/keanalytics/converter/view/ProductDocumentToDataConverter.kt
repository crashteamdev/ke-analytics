package dev.crashteam.keanalytics.converter.view

import dev.crashteam.keanalytics.controller.model.ProductItemView
import dev.crashteam.keanalytics.controller.model.ProductSellerView
import dev.crashteam.keanalytics.controller.model.ProductView
import dev.crashteam.keanalytics.converter.DataConverter
import dev.crashteam.keanalytics.domain.mongo.ProductDocument
import org.springframework.context.annotation.Lazy
import org.springframework.core.convert.ConversionService
import org.springframework.stereotype.Component

@Component
class ProductDocumentToDataConverter(
    @Lazy private val conversionService: ConversionService
) : DataConverter<ProductDocument, ProductView> {

    override fun convert(source: ProductDocument): ProductView? {
        return ProductView(
            productId = source.productId,
            title = source.title,
            parentCategory = source.parentCategory,
            ancestorCategories = source.ancestorCategories,
            reviewsAmount = source.reviewsAmount,
            orderAmount = source.orderAmount,
            rOrdersAmount = source.rOrdersAmount ?: source.orderAmount,
            totalAvailableAmount = source.totalAvailableAmount,
            description = source.description,
            attributes = source.attributes,
            tags = source.tags,
            seller = conversionService.convert(source.seller, ProductSellerView::class.java)!!,
            items = source.split?.map { conversionService.convert(it, ProductItemView::class.java)!! },
        )
    }
}
