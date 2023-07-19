package dev.crashteam.keanalytics.stream.listener

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import dev.crashteam.keanalytics.converter.clickhouse.ChKeProductConverterResultWrapper
import dev.crashteam.keanalytics.domain.mongo.*
import dev.crashteam.keanalytics.repository.clickhouse.CHProductRepository
import dev.crashteam.keanalytics.repository.mongo.ProductChangeTimeSeriesRepository
import dev.crashteam.keanalytics.repository.mongo.SellerRepository
import dev.crashteam.keanalytics.service.ProductService
import dev.crashteam.keanalytics.service.model.ProductDocumentTimeWrapper
import dev.crashteam.keanalytics.stream.model.KeItemSkuStreamRecord
import dev.crashteam.keanalytics.stream.model.KeProductCategoryStreamRecord
import dev.crashteam.keanalytics.stream.model.KeProductItemStreamRecord
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import mu.KotlinLogging
import org.springframework.core.convert.ConversionService
import org.springframework.data.redis.connection.stream.ObjectRecord
import org.springframework.stereotype.Component

private val log = KotlinLogging.logger {}

@Component
class KeProductItemStreamListener(
    private val objectMapper: ObjectMapper,
    private val productService: ProductService,
    private val conversionService: ConversionService,
    private val productChangeTimeSeriesRepository: ProductChangeTimeSeriesRepository,
    private val sellerRepository: SellerRepository,
    private val chProductRepository: CHProductRepository,
) : BatchStreamListener<String, ObjectRecord<String, String>> {

    override suspend fun onMessage(messages: List<ObjectRecord<String, String>>) {
        val keProductItemStreamRecords = messages.map {
            objectMapper.readValue<KeProductItemStreamRecord>(it.value)
        }
        log.info { "Consumer product records count ${keProductItemStreamRecords.size}" }
        coroutineScope {
            // OLD schema
            val oldSaveProductTask = async {
                try {
                    log.info { "Save ${keProductItemStreamRecords.size} products (OLD SCHEMA)" }
                    val productDocuments = keProductItemStreamRecords.map {
                        ProductDocumentTimeWrapper(
                            productDocument = toOldProductDocument(it),
                            time = it.time
                        )
                    }
                    productService.saveProductWithHistory(productDocuments)
                } catch (e: Exception) {
                    log.error(e) { "Exception during save products on OLD SCHEMA" }
                }
            }

            // NEW schema (Clickhouse)
            val saveProductTask = async {
                try {
                    log.info { "Save ${keProductItemStreamRecords.size} products (NEW)" }
                    val products = keProductItemStreamRecords.map {
                        conversionService.convert(it, ChKeProductConverterResultWrapper::class.java)!!
                    }.flatMap { it.result }
                    chProductRepository.saveProducts(products)
                } catch (e: Exception) {
                    log.error(e) { "Exception during save products on NEW SCHEMA" }
                }
            }

            val sellerTask = async {
                try {
                    val sellerDetailDocuments = keProductItemStreamRecords.map {
                        SellerDetailDocument(
                            sellerId = it.seller.id,
                            accountId = it.seller.accountId,
                            title = it.seller.sellerTitle,
                            link = it.seller.sellerLink
                        )
                    }.toSet()
                    sellerRepository.saveSellerBatch(sellerDetailDocuments).subscribe()
                } catch (e: Exception) {
                    log.error(e) { "Exception during save seller info" }
                }
            }
            awaitAll(oldSaveProductTask, saveProductTask, sellerTask)
        }
    }

    private fun toOldProductDocument(productRecord: KeProductItemStreamRecord): ProductDocument {
        return ProductDocument(
            productId = productRecord.productId,
            title = productRecord.title,
            parentCategory = productRecord.category.title.trim(),
            ancestorCategories = productCategoriesToAncestorCategories(productRecord.category),
            reviewsAmount = productRecord.reviewsAmount,
            orderAmount = productRecord.orders,
            rOrdersAmount = null,
            rating = productRecord.rating.toBigDecimal(),
            totalAvailableAmount = productRecord.totalAvailableAmount,
            description = productRecord.description,
            attributes = productRecord.attributes,
            tags = productRecord.tags,
            split = productRecord.skuList.map { productSku: KeItemSkuStreamRecord ->
                productSplitToMongoDomain(productSku)
            },
            seller = SellerDocument(
                id = productRecord.seller.id,
                title = productRecord.seller.sellerTitle,
                link = productRecord.seller.sellerLink,
                description = productRecord.seller.description,
                rating = productRecord.seller.rating.toBigDecimal(),
                sellerAccountId = productRecord.seller.accountId,
                contacts = productRecord.seller.contacts.map { ProductContactDocument(it.type, it.value) }
            ),
        )
    }

    private fun productSplitToMongoDomain(productSku: KeItemSkuStreamRecord): ProductSplitDocument {
        return ProductSplitDocument(
            id = productSku.skuId,
            availableAmount = productSku.availableAmount,
            fullPrice = productSku.fullPrice?.toBigDecimal(),
            purchasePrice = productSku.purchasePrice.toBigDecimal(),
            characteristics = productSku.characteristics.map {
                ProductSplitCharacteristicDocument(it.title, it.title, it.value)
            },
            photoKey = productSku.photoKey
        )
    }

    private fun productCategoriesToAncestorCategories(productCategory: KeProductCategoryStreamRecord): List<String> {
        val ancestorCategories: MutableList<String> = ArrayList()
        var currentCategory: KeProductCategoryStreamRecord? = productCategory
        while (currentCategory != null) {
            ancestorCategories.add(currentCategory.title.trim())
            currentCategory = currentCategory.parent
        }

        return ancestorCategories
    }
}
