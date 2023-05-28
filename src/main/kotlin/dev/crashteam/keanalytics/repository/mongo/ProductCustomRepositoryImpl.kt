package dev.crashteam.keanalytics.repository.mongo

import com.google.common.base.CaseFormat
import com.mongodb.BasicDBObject
import com.mongodb.bulk.BulkWriteResult
import com.mongodb.client.model.Filters
import com.mongodb.client.model.UpdateOneModel
import com.mongodb.client.model.UpdateOptions
import com.mongodb.client.result.UpdateResult
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactor.awaitSingle
import dev.crashteam.keanalytics.domain.mongo.ProductChangeHistoryDocument
import dev.crashteam.keanalytics.domain.mongo.ProductDocument
import dev.crashteam.keanalytics.domain.mongo.ProductSkuData
import dev.crashteam.keanalytics.repository.mongo.model.*
import dev.crashteam.keanalytics.repository.mongo.pageable.PageResult
import dev.crashteam.keanalytics.repository.mongo.pageable.ProductHistoryPageResult
import dev.crashteam.keanalytics.repository.mongo.pageable.ProductHistoryResultAggr
import org.bson.Document
import org.reactivestreams.Publisher
import org.springframework.data.domain.Pageable
import org.springframework.data.domain.Sort
import org.springframework.data.mongodb.core.ReactiveMongoTemplate
import org.springframework.data.mongodb.core.aggregation.*
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.TextCriteria
import org.springframework.data.mongodb.core.query.Update
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toMono
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

@Component
class ProductCustomRepositoryImpl(
    private val reactiveMongoTemplate: ReactiveMongoTemplate,
) : ProductCustomRepository {

    override fun saveProduct(product: ProductDocument): Mono<UpdateResult> {
        val query = Query().apply { addCriteria(Criteria.where("productId").`is`(product.productId)) }
        val update = Update().apply {
            set("productId", product.productId)
            set("title", product.title)
            set("parentCategory", product.parentCategory)
            set("ancestorCategories", product.ancestorCategories)
            set("reviewsAmount", product.reviewsAmount)
            set("orderAmount", product.orderAmount)
            set("rOrdersAmount", product.rOrdersAmount)
            set("totalAvailableAmount", product.totalAvailableAmount)
            set("description", product.description)
            set("attributes", product.attributes)
            set("tags", product.tags)
            set("seller", product.seller)
            set("split", product.split)
            setOnInsert("createdAt", LocalDateTime.now())
        }

        return reactiveMongoTemplate.upsert(query, update, ProductDocument::class.java)
    }

    override suspend fun saveProductBatch(products: Collection<ProductDocument>): Publisher<BulkWriteResult> {
        val updates = products.map { product ->
            val splitDocuments = product.split?.map {
                val splitDocument = Document()
                reactiveMongoTemplate.converter.write(it, splitDocument)
                splitDocument
            }
            val sellerDocument = Document()
            reactiveMongoTemplate.converter.write(product.seller, sellerDocument)
            UpdateOneModel<Document>(
                Filters.eq("productId", product.productId),
                BasicDBObject(
                    "\$set",
                    Document("productId", product.productId)
                        .append("title", product.title)
                        .append("parentCategory", product.parentCategory)
                        .append("ancestorCategories", product.ancestorCategories)
                        .append("reviewsAmount", product.reviewsAmount)
                        .append("orderAmount", product.orderAmount)
                        .append("rOrdersAmount", product.rOrdersAmount)
                        .append("totalAvailableAmount", product.totalAvailableAmount)
                        .append("description", product.description)
                        .append("attributes", product.attributes)
                        .append("tags", product.tags)
                        .append("seller", sellerDocument)
                        .append("split", splitDocuments)
                        .append("createdAt", LocalDateTime.now())
                ),
                UpdateOptions().upsert(true)
            )
        }
        val collectionName = reactiveMongoTemplate.getCollectionName(ProductDocument::class.java)
        return reactiveMongoTemplate.getCollection(collectionName).awaitSingle().bulkWrite(updates)
    }

    override fun findProduct(productId: Long): Mono<ProductDocument> {
        val query = Query().apply { addCriteria(Criteria.where("productId").`is`(productId)) }

        return reactiveMongoTemplate.findOne(query, ProductDocument::class.java)
    }

    override fun findShopProducts(shopName: String): Flux<ProductDocument> {
        val query = Query().apply { addCriteria(Criteria.where("seller.link").`is`(shopName)) }

        return reactiveMongoTemplate.find(query, ProductDocument::class.java)
    }

    override fun findTopShopsByTotalOrders(): Flux<ShopTotalOrder> {
        val group = Aggregation.group("seller.link", "seller.title")
            .sum("orderAmount").`as`("shopOrderAmount")
        val filter = Aggregation.match(Criteria.where("seller").ne(null))
        val sort = Aggregation.sort(Sort.by(Sort.Direction.DESC, "shopOrderAmount"))
        val project = Aggregation.project("shopOrderAmount").and("seller").previousOperation()
        val aggr: TypedAggregation<ProductDocument> =
            Aggregation.newAggregation(ProductDocument::class.java, listOf(filter, group, project, sort))

        return reactiveMongoTemplate.aggregate(aggr, ProductDocument::class.java, ShopTotalOrder::class.java)
    }

    override fun findTopProductByOrders(limit: Long): Flux<ProductTotalOrderAggregate> {
        val group = Aggregation.group("productId", "title")
            .sum("orderAmount").`as`("productOrderAmount")
            .push("seller").`as`("seller")
        val sort = Aggregation.sort(Sort.by(Sort.Direction.DESC, "productOrderAmount"))
        val project = Aggregation.project("productOrderAmount", "title")
            .and("productId").`as`("productId")
            .and("seller.title").`as`("seller")
        val unwind = Aggregation.unwind("seller")
        val aggrLimit = Aggregation.limit(limit)

        val options = AggregationOptions.builder().allowDiskUse(true).build()
        val aggr: TypedAggregation<ProductDocument> =
            Aggregation.newAggregation(ProductDocument::class.java, listOf(group, project, unwind, sort, aggrLimit))
                .withOptions(options)

        return reactiveMongoTemplate.aggregate(
            aggr,
            ProductDocument::class.java,
            ProductTotalOrderAggregate::class.java
        )
    }

    override fun findProductByProperties(
        filter: FindProductFilter,
        sort: Array<String>,
        page: Pageable,
    ): Mono<PageResult<ProductDocument>> {
        val query = Query().with(page)
        if (filter.productName?.isNotEmpty() == true) {
            query.addCriteria(TextCriteria.forDefaultLanguage().matching(filter.productName))
        }
        val criteriaList = mutableListOf<Criteria>().apply {
            if (filter.category?.isNotEmpty() == true) {
                add(Criteria.where("ancestorCategories").`in`(filter.category))
            }
            if (filter.sellerName?.isNotEmpty() == true) {
                add(Criteria.where("seller.title").regex("^${filter.sellerName}"))
            }
            if (filter.sellerLink?.isNotEmpty() == true) {
                add(Criteria.where("seller.link").regex("^${filter.sellerLink}"))
            }
            if (filter.orderAmountGt != null) {
                add(Criteria.where("orderAmount").gte(filter.orderAmountGt))
            }
            if (filter.orderAmountLt != null) {
                add(Criteria.where("orderAmount").lte(filter.orderAmountLt))
            }
        }

        if (criteriaList.isNotEmpty()) {
            query.addCriteria(Criteria().andOperator(*criteriaList.toTypedArray()))
        }

        // Sort block
        val orders = convertToOrders(sort)
        query.with(Sort.by(orders))

        val pageResult = PageResult<ProductDocument>()
        pageResult.page = page.pageNumber
        pageResult.pageSize = page.pageSize.toLong()
        return reactiveMongoTemplate.count(Query.of(query).limit(-1).skip(-1), ProductDocument::class.java).map {
            pageResult.totalPages = it.toInt() / page.pageSize
        }.flatMapMany {
            reactiveMongoTemplate.find(query, ProductDocument::class.java)
        }.collectList().map {
            pageResult.result = it
            pageResult
        }
    }

    override suspend fun getProductOrders(
        productId: Long,
        fromTime: LocalDateTime,
        toTime: LocalDateTime,
    ): Mono<ProductTotalOrdersAggregate> {
        val where = Criteria.where("_id.productId").`is`(productId)
        val match = Aggregation.match(where)
        val filterProject = Aggregation.project("productId", "skuId")
            .and("parentCategory").`as`("parentCategory")
            .and("ancestorCategories").`as`("ancestorCategories")
            .and("seller").`as`("seller")
            .and(
                ArrayOperators.Filter.filter("skuChange").`as`("skuChange").by(
                    BooleanOperators.And.and(
                        ComparisonOperators.Gte.valueOf("skuChange.date")
                            .greaterThanEqualTo(DateOperators.dateFromString(fromTime.toString())),
                        ComparisonOperators.Lte.valueOf("skuChange.date")
                            .lessThanEqualTo(DateOperators.dateFromString(toTime.toString()))
                    )
                )
            ).`as`("skuChange")
        val matchWithoutEmptyDate = Aggregation.match(Criteria.where("skuChange").not().size(0))
        val firstProjection = Aggregation.project("productId", "skuId")
            .and("seller").`as`("seller")
            .and(AccumulatorOperators.Max.maxOf("skuChange.totalOrderAmount")).`as`("maxTotalOrderAmount")
            .and(AccumulatorOperators.Min.minOf("skuChange.totalOrderAmount")).`as`("minTotalOrderAmount")
            .and("skuChange").arrayElementAt(1).`as`("firstSkuChange")
            .and("skuChange").arrayElementAt(-1).`as`("lastSkuChange")
            .and("skuChange").`as`("skuChange")

        val secondProjection = Aggregation.project("productId", "skuId")
            .and("seller").`as`("seller")
            .and("maxTotalOrderAmount").minus("minTotalOrderAmount").`as`("totalOrderAmount")
            .and("firstSkuChange.skuAvailableAmount").`as`("firstSkuQuantity")
            .and("lastSkuChange.skuAvailableAmount").`as`("lastSkuQuantity")
            .and(ConvertOperators.valueOf("lastSkuChange.price").convertToDecimal()).`as`("lastSkuPrice")
            .and(AccumulatorOperators.Avg.avgOf("skuChange.price")).`as`("avgPrice")

        val group = Aggregation.group("productId")
            .avg(ConditionalOperators.ifNull("avgPrice").then(Fields.field("lastSkuPrice"))).`as`("price")
            .first("totalOrderAmount").`as`("totalOrderAmount")
            .sum("lastSkuQuantity").`as`("quantity")
            .first("seller").`as`("seller")

        val aggr: TypedAggregation<ProductChangeHistoryDocument> =
            Aggregation.newAggregation(
                ProductChangeHistoryDocument::class.java,
                listOf(
                    match, filterProject, matchWithoutEmptyDate, firstProjection, secondProjection, group,
                    Aggregation.project("price", "totalOrderAmount", "quantity", "seller")
                        .and(
                            ArithmeticOperators.Multiply.valueOf("price").multiplyBy("totalOrderAmount")
                        ).`as`("earnings")
                        .and(
                            ArithmeticOperators.Divide.valueOf("totalOrderAmount")
                                .divideBy(ChronoUnit.DAYS.between(fromTime, toTime))
                        ).`as`("dailyOrder")
                )
            )

        return reactiveMongoTemplate.aggregate(
            aggr,
            ProductChangeHistoryDocument::class.java,
            ProductTotalOrdersAggregate::class.java
        ).toMono()
    }

    override suspend fun findProductHistoryByProductId(
        productId: LongArray,
        fromTime: LocalDateTime,
        toTime: LocalDateTime,
    ): Flux<MultipleProductHistorySales> {
        val where = Criteria.where("_id.productId").`in`(productId.toList())
        val match = Aggregation.match(where)
        val project = Aggregation.project()
            .and("seller").`as`("seller")
            .and(
                ArrayOperators.Filter.filter("skuChange").`as`("skuChange").by(
                    BooleanOperators.And.and(
                        ComparisonOperators.Gte.valueOf("skuChange.date")
                            .greaterThanEqualTo(DateOperators.dateFromString(fromTime.toString())),
                        ComparisonOperators.Lte.valueOf("skuChange.date")
                            .lessThanEqualTo(DateOperators.dateFromString(toTime.toString()))
                    )
                )
            ).`as`("skuChange")
        val aggr: TypedAggregation<ProductChangeHistoryDocument> =
            Aggregation.newAggregation(
                ProductChangeHistoryDocument::class.java,
                listOf(match, project, project)
            )

        return reactiveMongoTemplate.aggregate(
            aggr,
            ProductChangeHistoryDocument::class.java,
            MultipleProductHistorySales::class.java
        )
    }

    override suspend fun findProductHistoryBySkuId(
        productId: Long,
        skuId: Long,
        fromTime: LocalDateTime,
        toTime: LocalDateTime,
        page: Pageable,
    ): ProductHistoryPageResult {
        val where = Criteria.where("_id.productId").`is`(productId).and("_id.skuId").`is`(skuId)
        val match = Aggregation.match(where)
        val project = Aggregation.project()
            .and(
                ArrayOperators.Filter.filter("skuChange").`as`("skuChange").by(
                    BooleanOperators.And.and(
                        ComparisonOperators.Gte.valueOf("skuChange.date")
                            .greaterThanEqualTo(DateOperators.dateFromString(fromTime.toString())),
                        ComparisonOperators.Lte.valueOf("skuChange.date")
                            .lessThanEqualTo(DateOperators.dateFromString(toTime.toString()))
                    )
                )
            ).`as`("skuChange")
        val finalProject = Aggregation.project()
            .and("skuChange").`as`("skuChange")
            .and("skuChange").size().`as`("count")
            .and("skuChange").slice(page.pageSize, page.pageNumber * page.pageSize)
        val aggr: TypedAggregation<ProductChangeHistoryDocument> =
            Aggregation.newAggregation(
                ProductChangeHistoryDocument::class.java,
                listOf(match, project, finalProject)
            )

        val aggrResult = reactiveMongoTemplate.aggregate(
            aggr, ProductChangeHistoryDocument::class.java, ProductHistoryResultAggr::class.java
        ).awaitFirstOrNull() ?: return ProductHistoryPageResult(productId, skuId, null)

        val pageResult = PageResult<ProductSkuData>()
        pageResult.page = page.pageNumber
        pageResult.pageSize = page.pageSize.toLong()
        pageResult.totalPages = aggrResult.count / page.pageSize
        pageResult.result = aggrResult.skuChange

        return ProductHistoryPageResult(productId, skuId, pageResult)
    }

    override suspend fun findProductHistoryByCategory(
        category: String,
        fromTime: LocalDateTime,
        toTime: LocalDateTime,
        sort: Array<String>,
        page: Pageable,
    ): Flux<ProductCategoryAggregate> {
        val skip = page.pageNumber * page.pageSize.toLong()
        val where = Criteria.where("ancestorCategories").`is`(category)
        val match = Aggregation.match(where)
        val project = Aggregation.project()
            .and("parentCategory").`as`("parentCategory")
            .and("ancestorCategories").`as`("ancestorCategories")
            .and("seller").`as`("seller")
            .and(
                ArrayOperators.Filter.filter("skuChange").`as`("skuChange").by(
                    BooleanOperators.And.and(
                        ComparisonOperators.Gte.valueOf("skuChange.date")
                            .greaterThanEqualTo(DateOperators.dateFromString(fromTime.toString())),
                        ComparisonOperators.Lte.valueOf("skuChange.date")
                            .lessThanEqualTo(DateOperators.dateFromString(toTime.toString()))
                    )
                )
            ).`as`("skuChange")
        val matchWithoutEmptyDate = Aggregation.match(Criteria.where("skuChange").not().size(0))
        val facet = Aggregation.facet(Aggregation.count().`as`("total")).`as`("metadata")
            .and(
                Aggregation.skip(skip),
                Aggregation.limit(page.pageSize.toLong())
            ).`as`("data")
        val unwindMeta = Aggregation.unwind("metadata")
        val finalProject = Aggregation.project("data")
            .and("metadata.total").`as`("meta.total")
            .and(LiteralOperators.valueOf((skip / page.pageSize) + 1).asLiteral()).`as`("meta.page")
            .and(
                ArithmeticOperators.valueOf(
                    ArithmeticOperators.Divide.valueOf("metadata.total")
                        .divideBy(page.pageSize)
                ).ceil()
            ).`as`("meta.pages")

        val orders = convertToOrders(sort) {
            if (it == "product_id" || it == "sku_id") {
                "id.${CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, it)}"
            }
            "skuChange.${CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, it)}"
        }
        val sort = Aggregation.sort(Sort.by(orders))

        val options = AggregationOptions.builder().allowDiskUse(true).build()
        val aggr: TypedAggregation<ProductChangeHistoryDocument> =
            Aggregation.newAggregation(
                ProductChangeHistoryDocument::class.java,
                listOf(match, project, matchWithoutEmptyDate, sort, facet, unwindMeta, finalProject)
            ).withOptions(options)

        return reactiveMongoTemplate.aggregate(
            aggr,
            ProductChangeHistoryDocument::class.java,
            ProductCategoryAggregate::class.java
        )
    }

    override suspend fun findProductHistoryByCategoryWithLimit(
        category: String,
        fromTime: LocalDateTime,
        toTime: LocalDateTime,
        limit: Int
    ): Mono<MutableList<ProductHistorySkuAggregate>> {
        val match = Aggregation.match(Criteria.where("ancestorCategories").`is`(category))
        val project = Aggregation.project()
            .and("_id").`as`("id")
            .and("parentCategory").`as`("parentCategory")
            .and("ancestorCategories").`as`("ancestorCategories")
            .and("seller").`as`("seller")
            .and(
                ArrayOperators.Filter.filter("skuChange").`as`("skuChange").by(
                    BooleanOperators.And.and(
                        ComparisonOperators.Gte.valueOf("skuChange.date")
                            .greaterThanEqualTo(DateOperators.dateFromString(fromTime.toString())),
                        ComparisonOperators.Lte.valueOf("skuChange.date")
                            .lessThanEqualTo(DateOperators.dateFromString(toTime.toString()))
                    )
                )
            ).`as`("skuChange")
        val matchWithoutEmptyDate = Aggregation.match(Criteria.where("skuChange").not().size(0))
        val unwind = Aggregation.unwind("skuChange")
        val group = Aggregation.group("id.productId", "id.skuId")
            .sum("skuChange.totalOrderAmount").`as`("sumOrderAmount")
            .first("parentCategory").`as`("parentCategory")
            .first("ancestorCategories").`as`("ancestorCategories")
            .first("seller").`as`("seller")
            .push("skuChange").`as`("skuChange")
        val matchGreaterThanZero = Aggregation.match(Criteria.where("sumOrderAmount").gt(0))
        val sort = Aggregation.sort(
            Sort.by(
                listOf(
                    Sort.Order(Sort.Direction.DESC, "sumOrderAmount")
                )
            )
        )
        val aggrLimit = Aggregation.limit(limit.toLong())
        val options = AggregationOptions.builder().allowDiskUse(true).build()
        val aggr: TypedAggregation<ProductChangeHistoryDocument> = Aggregation.newAggregation(
            ProductChangeHistoryDocument::class.java,
            listOf(match, project, matchWithoutEmptyDate, unwind, group, matchGreaterThanZero, sort, aggrLimit)
        ).withOptions(options)

        return reactiveMongoTemplate.aggregate(
            aggr,
            ProductChangeHistoryDocument::class.java,
            ProductHistorySkuAggregate::class.java
        ).collectList()
    }

    override suspend fun findProductHistoryBySellerPageable(
        title: String?,
        link: String?,
        fromTime: LocalDateTime,
        toTime: LocalDateTime,
        sort: Array<String>,
        page: Pageable,
    ): Flux<ProductSellerAggregate> {
        val where: Criteria = if (title != null && link != null) {
            Criteria.where("seller.title").`is`(title).and("seller.link").`is`(link)
        } else if (title != null) {
            Criteria.where("seller.title").`is`(title)
        } else if (link != null) {
            Criteria.where("seller.link").`is`(link)
        } else {
            throw IllegalArgumentException("title and link can't be null")
        }
        val match = Aggregation.match(where)
        val project = Aggregation.project()
            .and("parentCategory").`as`("parentCategory")
            .and("ancestorCategories").`as`("ancestorCategories")
            .and("seller").`as`("seller")
            .and(
                ArrayOperators.Filter.filter("skuChange").`as`("skuChange").by(
                    BooleanOperators.And.and(
                        ComparisonOperators.Gte.valueOf("skuChange.date")
                            .greaterThanEqualTo(DateOperators.dateFromString(fromTime.toString())),
                        ComparisonOperators.Lte.valueOf("skuChange.date")
                            .lessThanEqualTo(DateOperators.dateFromString(toTime.toString()))
                    )
                )
            ).`as`("skuChange")
        val matchWithoutEmptyDate = Aggregation.match(Criteria.where("skuChange").not().size(0))
        val skip = page.pageNumber * page.pageSize.toLong()
        val facet = Aggregation.facet(Aggregation.count().`as`("total")).`as`("metadata")
            .and(
                Aggregation.skip(skip),
                Aggregation.limit(page.pageSize.toLong())
            ).`as`("data")
        val unwindMeta = Aggregation.unwind("metadata")
        val finalProject = Aggregation.project("data")
            .and("metadata.total").`as`("meta.total")
            .and(LiteralOperators.valueOf((skip / page.pageSize) + 1).asLiteral()).`as`("meta.page")
            .and(
                ArithmeticOperators.valueOf(
                    ArithmeticOperators.Divide.valueOf("metadata.total")
                        .divideBy(page.pageSize)
                ).ceil()
            ).`as`("meta.pages")

        val orders = convertToOrders(sort) {
            if (it == "product_id" || it == "sku_id") {
                "id.${CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, it)}"
            }
            "skuChange.${CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, it)}"
        }
        val sort = Aggregation.sort(Sort.by(orders))

        val options = AggregationOptions.builder().allowDiskUse(true).build()
        val aggr: TypedAggregation<ProductChangeHistoryDocument> =
            Aggregation.newAggregation(
                ProductChangeHistoryDocument::class.java,
                listOf(match, project, matchWithoutEmptyDate, sort, facet, unwindMeta, finalProject)
            ).withOptions(options)

        return reactiveMongoTemplate.aggregate(
            aggr,
            ProductChangeHistoryDocument::class.java,
            ProductSellerAggregate::class.java
        )
    }

    override suspend fun findProductHistoryBySeller(
        title: String?,
        link: String?,
        fromTime: LocalDateTime,
        toTime: LocalDateTime
    ): Flux<ProductHistorySkuAggregate> {
        val where: Criteria = if (title != null && link != null) {
            Criteria.where("seller.title").`is`(title).and("seller.link").`is`(link)
        } else if (title != null) {
            Criteria.where("seller.title").`is`(title)
        } else if (link != null) {
            Criteria.where("seller.link").`is`(link)
        } else {
            throw IllegalArgumentException("title and link can't be null")
        }
        val match = Aggregation.match(where)
        val project = Aggregation.project()
            .and("parentCategory").`as`("parentCategory")
            .and("ancestorCategories").`as`("ancestorCategories")
            .and("seller").`as`("seller")
            .and(
                ArrayOperators.Filter.filter("skuChange").`as`("skuChange").by(
                    BooleanOperators.And.and(
                        ComparisonOperators.Gte.valueOf("skuChange.date")
                            .greaterThanEqualTo(DateOperators.dateFromString(fromTime.toString())),
                        ComparisonOperators.Lte.valueOf("skuChange.date")
                            .lessThanEqualTo(DateOperators.dateFromString(toTime.toString()))
                    )
                )
            ).`as`("skuChange")
        val matchWithoutEmptyDate = Aggregation.match(Criteria.where("skuChange").not().size(0))
        val options = AggregationOptions.builder().allowDiskUse(true).build()
        val aggr: TypedAggregation<ProductChangeHistoryDocument> =
            Aggregation.newAggregation(
                ProductChangeHistoryDocument::class.java,
                listOf(match, project, matchWithoutEmptyDate)
            ).withOptions(options)

        return reactiveMongoTemplate.aggregate(
            aggr,
            ProductChangeHistoryDocument::class.java,
            ProductHistorySkuAggregate::class.java
        )
    }

    override suspend fun findDistinctSellerIds(): Flux<Long> {
        return reactiveMongoTemplate.query(ProductDocument::class.java)
            .distinct("seller._id").`as`(Long::class.java).all()
    }

    override suspend fun findProductIdsByCategory(category: String): Flux<Long> {
        val match = Aggregation.match(Criteria.where("ancestorCategories").`is`(category))
        val project = Aggregation.project("productId")
        val aggr: TypedAggregation<ProductDocument> = Aggregation.newAggregation(
            ProductDocument::class.java,
            listOf(match, project)
        )

        return reactiveMongoTemplate.aggregate(
            aggr,
            ProductDocument::class.java,
            SimpleIdsResult::class.java
        ).map { it.productId }
    }

    override suspend fun findSellerIdsByCategory(category: String): Flux<Long> {
        val match = Aggregation.match(
            Criteria.where("ancestorCategories").`is`(category).and("seller.id").ne(null)
        )
        val project = Aggregation.project("seller.id")
        val options = AggregationOptions.builder().allowDiskUse(true).build()
        val aggr: TypedAggregation<ProductDocument> = Aggregation.newAggregation(
            ProductDocument::class.java,
            listOf(match, project)
        ).withOptions(options)

        return reactiveMongoTemplate.aggregate(
            aggr,
            ProductDocument::class.java,
            SimpleIdsResult::class.java
        ).map { it.productId }
    }

    override suspend fun findProductIdsBySeller(sellerLink: String): Flux<Long> {
        val match = Aggregation.match(
            Criteria.where("seller.link").`is`(sellerLink)
        )
        val project = Aggregation.project("productId")
        val options = AggregationOptions.builder().allowDiskUse(true).build()
        val aggr: TypedAggregation<ProductDocument> = Aggregation.newAggregation(
            ProductDocument::class.java,
            listOf(match, project)
        ).withOptions(options)

        return reactiveMongoTemplate.aggregate(
            aggr,
            ProductDocument::class.java,
            SimpleIdsResult::class.java
        ).map { it.productId }
    }

    private fun convertToOrders(sort: Array<String>, transform: ((value: String) -> String)? = null): List<Sort.Order> {
        val orders: MutableList<Sort.Order> = ArrayList<Sort.Order>()
        if (sort[0].contains(",")) {
            for (sortOrder in sort) {
                val _sort = sortOrder.split(",".toRegex()).toTypedArray()
                val property = if (transform != null) {
                    transform(_sort[0])
                } else {
                    CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, _sort[0])
                }
                orders.add(Sort.Order(getSortDirection(_sort[1]), property))
            }
        } else {
            val property = if (transform != null) {
                transform(sort[0])
            } else {
                CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, sort[0])
            }
            orders.add(Sort.Order(getSortDirection(sort[1]), property))
        }
        return orders.toList()
    }

    private fun getSortDirection(direction: String): Sort.Direction {
        if (direction == "asc") {
            return Sort.Direction.ASC
        } else if (direction == "desc") {
            return Sort.Direction.DESC
        }
        return Sort.Direction.ASC
    }
}
