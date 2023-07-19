package dev.crashteam.keanalytics.converter.clickhouse

import dev.crashteam.keanalytics.converter.DataConverter
import dev.crashteam.keanalytics.repository.clickhouse.model.ChKazanExpressCharacteristic
import dev.crashteam.keanalytics.repository.clickhouse.model.ChKeProduct
import dev.crashteam.keanalytics.stream.model.KeProductCategoryStreamRecord
import dev.crashteam.keanalytics.stream.model.KeProductItemStreamRecord
import org.springframework.stereotype.Component
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.util.stream.Collectors

@Component
class UzumProductItemToChProductConverter :
    DataConverter<KeProductItemStreamRecord, ChKeProductConverterResultWrapper> {

    override fun convert(source: KeProductItemStreamRecord): ChKeProductConverterResultWrapper {
        return ChKeProductConverterResultWrapper(source.skuList.map { sku ->
            val fetchTime =
                LocalDateTime.ofInstant(Instant.ofEpochMilli(source.time), ZoneId.of("UTC"))
            ChKeProduct(
                fetchTime = fetchTime,
                productId = source.productId,
                skuId = sku.skuId,
                title = source.title,
                categoryPaths = categoryToPath(source.category),
                rating = source.rating.toBigDecimal(),
                reviewsAmount = source.reviewsAmount.toInt(),
                totalOrdersAmount = source.orders,
                totalAvailableAmount = source.totalAvailableAmount,
                availableAmount = sku.availableAmount,
                fullPrice = sku.fullPrice?.toBigDecimal()?.movePointRight(2)?.toLong(),
                purchasePrice = sku.purchasePrice.toBigDecimal().movePointRight(2).toLong(),
                attributes = source.attributes,
                tags = source.tags,
                photoKey = sku.photoKey,
                characteristics = sku.characteristics.map {
                    ChKazanExpressCharacteristic(it.type, it.title)
                },
                sellerId = source.seller.id,
                sellerAccountId = source.seller.accountId,
                sellerTitle = source.seller.sellerTitle,
                sellerLink = source.seller.sellerLink,
                sellerRegistrationDate = source.seller.registrationDate,
                sellerRating = source.seller.rating.toBigDecimal(),
                sellerReviewsCount = source.seller.reviews.toInt(),
                sellerOrders = source.seller.orders,
                sellerContacts = source.seller.contacts.stream()
                    .collect(Collectors.toMap({ it.type }, { it.value })),
                isEco = source.isEco,
                adultCategory = source.isAdult,
            )
        })

    }

    private fun categoryToPath(category: KeProductCategoryStreamRecord): List<Long> {
        val paths = mutableListOf<Long>()
        var nextCategory: KeProductCategoryStreamRecord? = category
        while (nextCategory != null) {
            paths.add(category.id)
            nextCategory = nextCategory.parent
        }
        return paths.toList()
    }
}
