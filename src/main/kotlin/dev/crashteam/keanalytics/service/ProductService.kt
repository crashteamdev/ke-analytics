package dev.crashteam.keanalytics.service

import com.mongodb.BasicDBObject
import com.mongodb.client.model.Filters
import com.mongodb.client.model.InsertOneModel
import com.mongodb.client.model.UpdateOneModel
import com.mongodb.client.model.WriteModel
import dev.crashteam.keanalytics.domain.mongo.*
import dev.crashteam.keanalytics.repository.clickhouse.CHProductPositionRepository
import dev.crashteam.keanalytics.repository.clickhouse.model.ChProductPositionHistory
import dev.crashteam.keanalytics.repository.mongo.CategoryRepository
import dev.crashteam.keanalytics.repository.mongo.ProductChangeHistoryDao
import dev.crashteam.keanalytics.repository.mongo.ProductPositionRepository
import dev.crashteam.keanalytics.repository.mongo.ProductRepository
import dev.crashteam.keanalytics.repository.mongo.model.*
import dev.crashteam.keanalytics.repository.mongo.pageable.PageResult
import dev.crashteam.keanalytics.service.calculator.ProductHistoryCalculator
import dev.crashteam.keanalytics.service.model.*
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactive.awaitLast
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import mu.KotlinLogging
import org.bson.Document
import org.springframework.data.domain.PageRequest
import org.springframework.data.mongodb.core.ReactiveMongoTemplate
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toMono
import java.math.BigDecimal
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.temporal.ChronoUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.stream.Stream
import kotlin.math.abs

private val log = KotlinLogging.logger {}

@Service
class ProductService(
    private val productRepository: ProductRepository,
    private val productChangeHistoryDao: ProductChangeHistoryDao,
    private val categoryRepository: CategoryRepository,
    private val productHistoryCalculator: ProductHistoryCalculator,
    private val productPositionRepository: ProductPositionRepository,
    private val reactiveMongoTemplate: ReactiveMongoTemplate,
    private val chProductPositionRepository: CHProductPositionRepository,
) {

    fun findProductByProperties(
        filter: FindProductFilter,
        sort: Array<String>,
        page: Int,
        size: Int,
    ): Mono<PageResult<ProductDocument>> {
        log.info { "Find products by properties: filter=$filter; page=$page; size=$size" }
        return productRepository.findProductByProperties(filter, sort, PageRequest.of(page, size))
    }

    fun getProduct(productId: Long): Mono<ProductDocument> {
        log.info { "Find product by productId=$productId" }
        return productRepository.findProduct(productId)
    }

    suspend fun getProductOrders(
        productId: Long,
        fromTime: LocalDateTime,
        toTime: LocalDateTime,
    ): ProductTotalOrdersAggregate? {
        log.info { "Get product orders by productId=$productId; fromTime=$fromTime; toTime=$toTime" }
        return productRepository.getProductOrders(productId, fromTime, toTime).awaitSingleOrNull()
    }

    suspend fun getProductsSales(
        productIds: LongArray,
        fromTime: LocalDateTime,
        toTime: LocalDateTime,
    ): List<ProductSales>? {
        log.info { "Get product sales by productIdsSize=${productIds.size}; fromTime=$fromTime; toTime=$toTime" }
        val productsHistoryList = productRepository.findProductHistoryByProductId(productIds, fromTime, toTime)
            .collectList()
            .awaitSingleOrNull()
        if (productsHistoryList == null || productsHistoryList.isEmpty()) {
            return null
        }
        val productSales = productsHistoryList.map { multipleProductHistorySales: MultipleProductHistorySales ->
            if (multipleProductHistorySales.skuChange.isEmpty()) {
                ProductSales(
                    productId = multipleProductHistorySales.id.productId,
                    skuId = multipleProductHistorySales.id.skuId,
                    seller = multipleProductHistorySales.seller.let {
                        ProductSalesSeller(it.title, it.link, it.accountId ?: it.sellerAccountId)
                    },
                    dayChange = emptyList(),
                )
            } else {
                val allVariationHistory = productsHistoryList.filter {
                    multipleProductHistorySales.id.productId == it.id.productId
                }
                val calculatedProductHistory: List<ProductSkuHistorical> = productHistoryCalculator.calculate(
                    multipleProductHistorySales.id.productId,
                    multipleProductHistorySales.id.skuId,
                    multipleProductHistorySales.skuChange,
                    allVariationHistory.map { ProductHistory(it.id, it.skuChange) }
                )
                ProductSales(
                    productId = multipleProductHistorySales.id.productId,
                    skuId = multipleProductHistorySales.id.skuId,
                    seller = multipleProductHistorySales.seller.let {
                        ProductSalesSeller(it.title, it.link, it.accountId ?: it.sellerAccountId)
                    },
                    dayChange = calculatedProductHistory.map {
                        ProductDayChange(
                            it.date!!,
                            it.orderAmount,
                            it.reviewsAmount,
                            it.price!!,
                            it.availableAmount!!,
                            it.salesAmount!!
                        )
                    }
                )
            }
        }
        return productSales
    }

    suspend fun getProductSkuSalesHistory(
        productId: Long,
        skuId: Long,
        fromTime: LocalDateTime,
        toTime: LocalDateTime,
        page: Int,
        size: Int,
    ): PageResult<ProductSkuHistorical>? {
        log.info { "Get product sales history by productId=$productId; skuId=$skuId; fromTime=$fromTime; toTime=$toTime" }
        val productHistoryDocuments = productRepository.findProductHistoryBySkuId(
            productId,
            skuId,
            fromTime,
            toTime.plusDays(1),
            PageRequest.of(page, size)
        )
        val pageResult: PageResult<ProductSkuData>? = productHistoryDocuments.result

        if (pageResult == null || pageResult.result.isEmpty()) {
            return null
        }

        // Find history of product with all variation (sku)
        val productChangeHistory = productChangeHistoryDao.findByProductId(productId).collectList().awaitLast()
        val calculateResult: List<ProductSkuHistorical> =
            productHistoryCalculator.calculate(
                productId,
                skuId,
                pageResult.result,
                productChangeHistory.map { ProductHistory(it.id, it.skuChange) }
            )

        return PageResult(calculateResult, pageResult.pageSize, pageResult.page, pageResult.totalPages)
    }

    suspend fun saveProductWithHistory(
        productDocumentWithTimeMark: List<ProductDocumentTimeWrapper>
    ) {
        // Save product
        productRepository.saveProductBatch(productDocumentWithTimeMark.map { it.productDocument }).awaitLast()

        // Save product history
        val documentOps = productDocumentWithTimeMark.flatMap {
            val productDateTime =
                LocalDateTime.ofInstant(Instant.ofEpochMilli(it.time), ZoneId.of("UTC"))
            buildProductDocumentOps(it.productDocument, productDateTime.toLocalDate())!!
        }
        val collectionName =
            reactiveMongoTemplate.getCollectionName(ProductChangeHistoryDocument::class.java)
        if (documentOps.isNotEmpty()) {
            val bulkWriteResult = reactiveMongoTemplate.getCollection(collectionName).awaitSingle()
                .bulkWrite(documentOps).awaitLast()
            log.info {
                "Batch product write result. inserterCount=${bulkWriteResult.insertedCount}; " +
                        "updatedCount=${bulkWriteResult.modifiedCount}"
            }
        }
    }

    suspend fun saveProductWithHistory(
        productDocuments: List<ProductDocument>,
        localDate: LocalDate = LocalDate.now()
    ) {
        // Save product
        productRepository.saveProductBatch(productDocuments).awaitLast()

        // Save product history
        val documentOps = productDocuments.flatMap { buildProductDocumentOps(it, localDate)!! }
        val collectionName =
            reactiveMongoTemplate.getCollectionName(ProductChangeHistoryDocument::class.java)
        if (documentOps.isNotEmpty()) {
            val bulkWriteResult = reactiveMongoTemplate.getCollection(collectionName).awaitSingle()
                .bulkWrite(documentOps).awaitLast()
            log.info {
                "Batch product write result. inserterCount=${bulkWriteResult.insertedCount}; " +
                        "updatedCount=${bulkWriteResult.modifiedCount}"
            }
        }
    }

    private suspend fun buildProductDocumentOps(
        productDocument: ProductDocument,
        productCollectDay: LocalDate,
    ): List<WriteModel<Document>>? {
        val documentOps = productDocument.split?.filter { productSplitDocument ->
            val isHistoryExists = productChangeHistoryDao.existsByProductIdSkuIdAndDay(
                productDocument.productId,
                productSplitDocument.id,
                productCollectDay
            ).awaitSingleOrNull() ?: false
            !isHistoryExists // Save once per day
        }?.map { productSplitDocument ->
            val productSkuData = ProductSkuData(
                date = Instant.now(),
                rating = productDocument.rating,
                reviewsAmount = productDocument.reviewsAmount,
                totalOrderAmount = productDocument.orderAmount,
                totalAvailableAmount = productDocument.totalAvailableAmount,
                skuAvailableAmount = productSplitDocument.availableAmount,
                skuCharacteristic = productSplitDocument.characteristics.map {
                    ProductSkuCharacteristic(it.type, it.title, it.value)
                },
                name = productDocument.title,
                price = productSplitDocument.purchasePrice,
                fullPrice = productSplitDocument.fullPrice,
                photoKey = productSplitDocument.photoKey
            )
            val productHistory: ProductChangeHistoryDocument? = productChangeHistoryDao.findByProductIdAndSkuId(
                productDocument.productId, productSplitDocument.id
            ).awaitSingleOrNull()
            val idDocument = Document()
                .append("productId", productDocument.productId)
                .append("skuId", productSplitDocument.id)
            val skuChangeDocument = Document()
            reactiveMongoTemplate.converter.write(productSkuData, skuChangeDocument)
            if (productHistory != null) {
                log.info {
                    "Build UPDATE model for productId=${productDocument.productId};" +
                            " skuId=${productSplitDocument.id}"
                }
                val sellerDocument = Document()
                reactiveMongoTemplate.converter.write(
                    SellerData(
                        productDocument.seller.title,
                        productDocument.seller.link,
                        productDocument.seller.sellerAccountId,
                        null
                    ),
                    sellerDocument
                )
                UpdateOneModel<Document>(
                    Filters.eq("_id", idDocument),
                    BasicDBObject(
                        "\$push", Document("skuChange", skuChangeDocument)
                    ).append(
                        "\$set", Document("parentCategory", productDocument.parentCategory)
                            .append("ancestorCategories", productDocument.ancestorCategories)
                            .append("seller", sellerDocument)
                    ),
                )
            } else {
                log.info {
                    "Build INSERT model for productId=${productDocument.productId};" +
                            " skuId=${productSplitDocument.id}"
                }
                val sellerDocument = Document()
                reactiveMongoTemplate.converter.write(
                    SellerData(
                        productDocument.seller.title,
                        productDocument.seller.link,
                        productDocument.seller.sellerAccountId,
                        null
                    ),
                    sellerDocument
                )
                InsertOneModel<Document>(
                    Document("_id", idDocument)
                        .append("parentCategory", productDocument.parentCategory)
                        .append("ancestorCategories", productDocument.ancestorCategories)
                        .append(
                            "seller",
                            sellerDocument
                        )
                        .append("skuChange", listOf(skuChangeDocument))
                )
            }
        }
        return documentOps
    }

    suspend fun getCategorySales(
        categoryPathIds: List<Long>,
        fromTime: LocalDateTime,
        toTime: LocalDateTime,
        sort: Array<String>,
        page: Int,
        limit: Int,
    ): Mono<AggregateSalesWrapper> {
        log.info { "Get category sales by categoryIds=$categoryPathIds; fromTime=$fromTime; toTime=$toTime" }
        val categoryDocument = if (categoryPathIds.size == 1) {
            categoryRepository.findByPublicId(categoryPathIds.first()).awaitSingleOrNull()
                ?: throw IllegalArgumentException("Invalid category id")
        } else {
            val path = buildCategoryPath(categoryPathIds)
            categoryRepository.findByPath(path).awaitFirstOrNull()
                ?: throw IllegalArgumentException("Invalid category path: $path")
        }
        val pageRequest = PageRequest.of(page, limit)
        val categoryHistory =
            productRepository.findProductHistoryByCategory(categoryDocument.title, fromTime, toTime, sort, pageRequest)
                .awaitFirstOrNull() ?: return Mono.empty()
        val aggregateProductHistory = calculateAggregateProductHistory(categoryHistory.data, fromTime, toTime)

        return AggregateSalesWrapper(
            aggregateProductHistory,
            AggregateSalesMetadata(
                categoryHistory.meta.total,
                categoryHistory.meta.page,
                categoryHistory.meta.pages,
                pageRequest.pageSize
            )
        ).toMono()
    }

    suspend fun getCategoryProductIds(
        categoryPathIds: List<Long>
    ): Flux<Long> {
        val categoryDocument = if (categoryPathIds.size == 1) {
            categoryRepository.findByPublicId(categoryPathIds.first()).awaitSingleOrNull()
                ?: throw IllegalArgumentException("Invalid category id")
        } else {
            val path = buildCategoryPath(categoryPathIds)
            categoryRepository.findByPath(path).awaitFirstOrNull()
                ?: throw IllegalArgumentException("Invalid category path: $path")
        }

        return productRepository.findProductIdsByCategory(categoryDocument.title)
    }

    suspend fun getCategorySellerIds(
        categoryPathIds: List<Long>
    ): Flux<Long> {
        val categoryDocument = if (categoryPathIds.size == 1) {
            categoryRepository.findByPublicId(categoryPathIds.first()).awaitSingleOrNull()
                ?: throw IllegalArgumentException("Invalid category id")
        } else {
            val path = buildCategoryPath(categoryPathIds)
            categoryRepository.findByPath(path).awaitFirstOrNull()
                ?: throw IllegalArgumentException("Invalid category path: $path")
        }

        return productRepository.findSellerIdsByCategory(categoryDocument.title)
    }

    suspend fun getSellerProductIdsByLink(sellerLink: String): Flux<Long> {
        return productRepository.findProductIdsBySeller(sellerLink)
    }

    suspend fun getCategorySalesWithLimit(
        categoryTitle: String,
        fromTime: LocalDateTime,
        toTime: LocalDateTime,
        limit: Int,
    ): List<AggregateSalesProduct> {
        log.info { "Get category sales by category=$categoryTitle; fromTime=$fromTime; toTime=$toTime" }
        val categoryProductHistory =
            productRepository.findProductHistoryByCategoryWithLimit(categoryTitle, fromTime, toTime, limit).awaitLast()

        return calculateAggregateProductHistory(categoryProductHistory, fromTime, toTime)
    }

    suspend fun getSellerSalesPageable(
        title: String?,
        link: String?,
        fromTime: LocalDateTime,
        toTime: LocalDateTime,
        sort: Array<String>,
        page: Int,
        limit: Int,
    ): Mono<AggregateSalesWrapper> {
        log.info { "Get seller sales by name=$title; fromTime=$fromTime; toTime=$toTime" }
        val pageRequest = PageRequest.of(page, limit)
        val historyBySeller =
            productRepository.findProductHistoryBySellerPageable(title, link, fromTime, toTime, sort, pageRequest)
                .awaitFirstOrNull()
                ?: return Mono.empty()
        val calculateAggregateProductHistory = calculateAggregateProductHistory(historyBySeller.data, fromTime, toTime)

        return AggregateSalesWrapper(
            calculateAggregateProductHistory,
            AggregateSalesMetadata(
                historyBySeller.meta.total,
                historyBySeller.meta.page,
                historyBySeller.meta.pages,
                pageRequest.pageSize
            )
        ).toMono()
    }

    suspend fun getSellerSales(
        title: String? = null,
        link: String? = null,
        fromTime: LocalDateTime,
        toTime: LocalDateTime,
    ): List<AggregateSalesProduct>? {
        log.info { "Get seller sales by name=$title; fromTime=$fromTime; toTime=$toTime" }
        val historyBySeller: MutableList<ProductHistorySkuAggregate> =
            productRepository.findProductHistoryBySeller(title, link, fromTime, toTime).collectList()
                .awaitSingleOrNull()
                ?: return null

        return calculateAggregateProductHistory(historyBySeller, fromTime, toTime)
    }

    suspend fun getProductPositions(
        categoryId: Long,
        productId: Long,
        skuId: Long,
        fromTime: LocalDateTime,
        toTime: LocalDateTime,
    ): List<ChProductPositionHistory> {
        log.info {
            "Get product positions. categoryId=$categoryId; productId=$productId; skuId=$skuId;" +
                    " fromTime=$fromTime; toTime=$toTime"
        }
        val productPositionHistory = chProductPositionRepository.getProductPositionHistory(
            categoryId.toString(),
            productId.toString(),
            skuId.toString(),
            fromTime,
            toTime
        )
        if (productPositionHistory.isEmpty()) {
            return emptyList()
        }

        return productPositionHistory
    }

    fun getProductPositions(
        categoryId: Long,
        productId: Long,
        skuId: Long,
        fromDate: LocalDate,
        toDate: LocalDate,
    ): List<ChProductPositionHistory> {
        log.info {
            "Get product positions. categoryId=$categoryId; productId=$productId; skuId=$skuId;" +
                    " fromDate=$fromDate; toDate=$toDate"
        }
        val productPositionHistory = chProductPositionRepository.getProductPositionHistory(
            categoryId.toString(),
            productId.toString(),
            skuId.toString(),
            fromDate.atStartOfDay(),
            toDate.atStartOfDay()
        )
        if (productPositionHistory.isEmpty()) {
            return emptyList()
        }

        return productPositionHistory
    }

    private fun calculateAggregateProductHistory(
        productHistory: List<ProductHistorySkuAggregate>,
        fromTime: LocalDateTime,
        toTime: LocalDateTime,
    ): List<AggregateSalesProduct> {
        val daysCount = ChronoUnit.DAYS.between(fromTime, toTime)
        return productHistory.map { productHistorySkuAggr ->
            val productSkuLinkList = productHistorySkuAggr.skuChange.mapIndexedNotNull { index, skuChange ->
                if ((index + 1) >= productHistorySkuAggr.skuChange.size) return@mapIndexedNotNull null
                val nextSkuChange = productHistorySkuAggr.skuChange[index + 1]
                ProductSkuDayLink(skuChange, nextSkuChange)
            }
            val daysInStock = AtomicInteger()
            productHistorySkuAggr.skuChange.forEach {
                if (it.skuAvailableAmount > 0) {
                    daysInStock.getAndIncrement()
                }
            }
            var proceeds = BigDecimal.ZERO
            productSkuLinkList.forEach {
                val availableAmountDiff = it.nextDaySku.skuAvailableAmount - it.sku.skuAvailableAmount
                val orderCount = if (availableAmountDiff < 0) {
                    BigDecimal.valueOf(abs(it.nextDaySku.skuAvailableAmount - it.sku.skuAvailableAmount))
                } else BigDecimal.ZERO
                proceeds = proceeds.plus(it.sku.price.multiply(orderCount))
            }

            val productHistorySkuChange = productHistorySkuAggr.skuChange.first()
            val productName = if (productHistorySkuChange.skuCharacteristic.isNotEmpty()) {
                val characteristicName =
                    productHistorySkuChange.skuCharacteristic.joinToString { "${it.type} ${it.title}" }
                "${productHistorySkuChange.name} $characteristicName"
            } else productHistorySkuChange.name
            val categoryName = productHistorySkuAggr.ancestorCategories.first()
            AggregateSalesProduct(
                productId = productHistorySkuAggr.id.productId,
                skuId = productHistorySkuAggr.id.skuId,
                name = productName,
                seller = CategorySalesSeller(
                    id = productHistorySkuAggr.seller.accountId ?: productHistorySkuAggr.seller.sellerAccountId,
                    name = productHistorySkuAggr.seller.title
                ),
                category = CategoryData(categoryName),
                availableAmount = productHistorySkuAggr.skuChange.last().skuAvailableAmount,
                price = productHistorySkuAggr.skuChange.last().price,
                proceeds = proceeds,
                priceGraph = buildPriceGraph(daysCount, productHistorySkuAggr.skuChange),
                orderGraph = buildOrderGraph(daysCount, productSkuLinkList),
                daysInStock = daysInStock.get()
            )
        }
    }

    suspend fun buildCategoryPath(categoryIds: List<Long>): String {
        val sb = StringBuilder()
        for (categoryId in categoryIds) {
            val title = categoryRepository.findByPublicId(categoryId).awaitSingle().title
            sb.append(",$title")
        }
        sb.append(",")
        return sb.toString()
    }

    private fun buildPriceGraph(daysCount: Long, skuChanges: List<ProductHistorySkuChange>): MutableList<Long> {
        val priceGraph = mutableListOf<Long>()
        for (i in 0 until daysCount) {
            if (i >= skuChanges.size) {
                priceGraph.add(0)
                continue
            }
            val skuChange = skuChanges[i.toInt()]
            priceGraph.add(skuChange.price.toLong())
        }
        return priceGraph
    }

    private fun buildOrderGraph(daysCount: Long, skuLinkList: List<ProductSkuDayLink>): MutableList<Long> {
        val orderGraph = mutableListOf<Long>()
        for (i in 0 until daysCount) {
            if (i >= skuLinkList.size) {
                orderGraph.add(0)
                continue
            }
            val skuDayLink = skuLinkList[i.toInt()]
            val skuChange = skuDayLink.sku
            val nextDaySkuChange = skuDayLink.nextDaySku
            val availableAmountDiff = nextDaySkuChange.skuAvailableAmount - skuChange.skuAvailableAmount
            val orderCount = if (availableAmountDiff < 0) {
                abs(nextDaySkuChange.skuAvailableAmount - skuChange.skuAvailableAmount)
            } else 0
            orderGraph.add(orderCount)
        }
        return orderGraph
    }
}
