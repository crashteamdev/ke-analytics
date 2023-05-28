package dev.crashteam.keanalytics.service.calculator

import dev.crashteam.keanalytics.domain.mongo.ProductSkuData
import dev.crashteam.keanalytics.service.model.ProductHistory
import dev.crashteam.keanalytics.service.model.ProductSkuHistorical
import dev.crashteam.keanalytics.service.model.ProductSkuHistoricalCharacteristic
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.math.RoundingMode
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

@Component
class ProductHistoryCalculator {

    fun calculate(
        productId: Long,
        skuId: Long,
        productHistory: List<ProductSkuData>,
        productFullHistory: List<ProductHistory>
    ): List<ProductSkuHistorical> {
        // Remove duplicate date (Hotfix)
        val productHistory = productHistory.filterIndexed { index, productSkuData ->
            if (index + 1 < productHistory.size) {
                val productDate = productSkuData.date
                val nextProductDate = productHistory[index + 1].date
                productDate.atOffset(ZoneOffset.UTC).toLocalDate() != nextProductDate.atOffset(ZoneOffset.UTC).toLocalDate()
            } else true
        }
        return productHistory.mapIndexedNotNull { index, productSkuData ->
            if (index >= (productHistory.size - 1)) return@mapIndexedNotNull null // Ignore last history record
            val nextDayProductData = productHistory.find {
                val productDate = productSkuData.date
                ChronoUnit.DAYS.between(
                    productDate.atOffset(ZoneOffset.UTC).toLocalDate(),
                    it.date.atOffset(ZoneOffset.UTC).toLocalDate()
                ) == 1L
            }
            if (nextDayProductData == null) {
                ProductSkuHistorical(
                    productId = productId,
                    skuId = skuId,
                    name = productSkuData.name,
                    orderAmount = 0,
                    reviewsAmount = productSkuData.reviewsAmount,
                    totalAvailableAmount = productSkuData.totalAvailableAmount
                ).apply {
                    fullPrice = productSkuData.fullPrice
                    price = productSkuData.price
                    availableAmount = productSkuData.skuAvailableAmount
                    salesAmount = productSkuData.price.multiply(BigDecimal.valueOf(0))
                    photoKey = productSkuData.photoKey
                    characteristic = productSkuData.skuCharacteristic?.map {
                        ProductSkuHistoricalCharacteristic(it.type, it.title, it.value)
                    } ?: emptyList()
                    date = productSkuData.date
                }
            } else {
                val productCalcData = ProductCalcData(
                    purchasePrice = nextDayProductData.price,
                    fullPrice = nextDayProductData.fullPrice,
                    reviewsAmount = nextDayProductData.reviewsAmount,
                    totalOrderAmount = nextDayProductData.totalOrderAmount,
                    totalAvailableAmount = nextDayProductData.totalAvailableAmount,
                    skuAvailableAmount = nextDayProductData.skuAvailableAmount
                )
                calculateProductData(productId, skuId, productSkuData, productCalcData, productFullHistory)
            }
        }
    }

    private fun calculateProductData(
        productId: Long,
        skuId: Long,
        productSkuData: ProductSkuData,
        nextProductSkuData: ProductCalcData,
        productFullHistory: List<ProductHistory>
    ): ProductSkuHistorical {
        val orderAvailabilityAmount = productSkuData.skuAvailableAmount
        val nextAvailableAmount = nextProductSkuData.skuAvailableAmount
        val dailyOrderAmount = if (nextProductSkuData.totalOrderAmount <= productSkuData.totalOrderAmount) {
            0L
        } else if (nextAvailableAmount > orderAvailabilityAmount) {
            val dailyOrderAmount = nextProductSkuData.totalOrderAmount - productSkuData.totalOrderAmount
            if (dailyOrderAmount <= 0L) {
                0L
            } else {
                // Compare with another product variation
                val anotherProductVariationHistoryList = productFullHistory.filter { it.id.skuId != skuId }
                val anotherItemsOrderAmount = AtomicLong(0L)
                val itemsWithHighAvailabilityCount = AtomicInteger(0)
                for (productChangeHistoryDocument in anotherProductVariationHistoryList) {
                    val productWithSameDateIndex = productChangeHistoryDocument.skuChange.indexOfFirst {
                        it.date == productSkuData.date
                    }

                    // Ignore if product history with same date not found
                    if (productWithSameDateIndex == -1) continue

                    if ((productWithSameDateIndex + 1) < productChangeHistoryDocument.skuChange.size) {
                        val prevDayProductHistory = productChangeHistoryDocument.skuChange[productWithSameDateIndex]
                        val nextDayProductHistory = productChangeHistoryDocument.skuChange[productWithSameDateIndex + 1]
                        if (nextDayProductHistory.skuAvailableAmount < prevDayProductHistory.skuAvailableAmount) {
                            val orderAmount =
                                prevDayProductHistory.skuAvailableAmount - nextDayProductHistory.skuAvailableAmount
                            anotherItemsOrderAmount.getAndUpdate { it + orderAmount }
                        } else {
                            // There is another item with higher available amount then prev
                            itemsWithHighAvailabilityCount.getAndIncrement()
                        }
                    }
                }
                if (itemsWithHighAvailabilityCount.get() != 0) {
                    dailyOrderAmount / (itemsWithHighAvailabilityCount.get() + 1)
                } else if (anotherItemsOrderAmount.get() != 0L && dailyOrderAmount > anotherItemsOrderAmount.get()) {
                    dailyOrderAmount - anotherItemsOrderAmount.get()
                } else {
                    dailyOrderAmount / (anotherProductVariationHistoryList.size + 1)
                }
            }
        } else {
            orderAvailabilityAmount - nextAvailableAmount
        }
        return ProductSkuHistorical(
            productId = productId,
            skuId = skuId,
            name = productSkuData.name,
            orderAmount = dailyOrderAmount,
            reviewsAmount = nextProductSkuData.reviewsAmount - productSkuData.reviewsAmount,
            totalAvailableAmount = nextProductSkuData.totalAvailableAmount
        ).apply {
            fullPrice = productSkuData.fullPrice
            price = productSkuData.price
            availableAmount = nextProductSkuData.skuAvailableAmount
            salesAmount = productSkuData.price.multiply(BigDecimal.valueOf(dailyOrderAmount))
            photoKey = productSkuData.photoKey
            characteristic = productSkuData.skuCharacteristic?.map {
                ProductSkuHistoricalCharacteristic(it.type, it.title, it.value)
            } ?: emptyList()
            date = productSkuData.date
        }
    }

    private data class ProductCalcData(
        val purchasePrice: BigDecimal,
        val fullPrice: BigDecimal?,
        val reviewsAmount: Long,
        val totalOrderAmount: Long,
        val totalAvailableAmount: Long,
        val skuAvailableAmount: Long,
    )
}
