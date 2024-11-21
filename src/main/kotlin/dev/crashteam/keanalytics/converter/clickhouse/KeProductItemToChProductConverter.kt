package dev.crashteam.keanalytics.converter.clickhouse

import dev.crashteam.ke.scrapper.data.v1.KeProductChange
import dev.crashteam.keanalytics.converter.DataConverter
import dev.crashteam.keanalytics.extensions.toLocalDateTime
import dev.crashteam.keanalytics.repository.clickhouse.model.ChKazanExpressCharacteristic
import dev.crashteam.keanalytics.repository.clickhouse.model.ChKeProduct
import dev.crashteam.keanalytics.stream.handler.aws.model.KeProductWrapper
import org.springframework.stereotype.Component
import java.util.stream.Collectors

@Component
class KeProductItemToChProductConverter :
    DataConverter<KeProductWrapper, ChKeProductConverterResultWrapper> {

    override fun convert(source: KeProductWrapper): ChKeProductConverterResultWrapper {
        val productChange = source.product
        return ChKeProductConverterResultWrapper(productChange.skusList.map { sku ->
            ChKeProduct(
                fetchTime = source.eventTime.toLocalDateTime(),
                productId = productChange.productId.toLong(),
                skuId = sku.skuId.toLong(),
                title = productChange.title,
                categoryPaths = categoryToPath(productChange.category),
                rating = productChange.rating.toBigDecimal(),
                reviewsAmount = productChange.reviewsAmount.toInt(),
                totalOrdersAmount = productChange.orders,
                totalAvailableAmount = productChange.totalAvailableAmount,
                availableAmount = sku.availableAmount,
                fullPrice = if (sku.fullPrice.isNotEmpty()) {
                    sku.fullPrice?.toBigDecimal()?.movePointRight(2)?.toLong()
                } else null,
                purchasePrice = sku.purchasePrice.toBigDecimal().movePointRight(2).toLong(),
                attributes = productChange.attributesList,
                tags = productChange.tagsList,
                photoKey = sku.photoKey,
                characteristics = sku.characteristicsList.map {
                    ChKazanExpressCharacteristic(it.type, it.title)
                },
                sellerId = productChange.seller.id,
                sellerAccountId = productChange.seller.accountId,
                sellerTitle = productChange.seller.sellerTitle,
                sellerLink = productChange.seller.sellerLink,
                sellerRegistrationDate = productChange.seller.registrationDate.seconds,
                sellerRating = productChange.seller.rating.toBigDecimal(),
                sellerReviewsCount = productChange.seller.reviews.toInt(),
                sellerOrders = productChange.seller.orders,
                sellerContacts = productChange.seller.contactsList.stream()
                    .collect(Collectors.toMap({ it.type }, { it.value })),
                isEco = productChange.isEco,
                adultCategory = productChange.isAdult,
            )
        })

    }

    private fun categoryToPath(category: KeProductChange.KeProductCategory): List<Long> {
        val paths = mutableListOf<Long>()
        var nextCategory: KeProductChange.KeProductCategory? = category
        while (nextCategory != null) {
            paths.add(category.id)
            if (!nextCategory.hasParent()) break
            nextCategory = nextCategory.parent
        }
        return paths.toList()
    }
}
