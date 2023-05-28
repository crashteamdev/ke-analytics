package dev.crashteam.keanalytics.repository.mongo

import dev.crashteam.keanalytics.domain.mongo.ProductChangeTimeSeries
import dev.crashteam.keanalytics.repository.mongo.model.*
import org.bson.Document
import org.springframework.data.mongodb.core.ReactiveMongoTemplate
import org.springframework.data.mongodb.core.aggregation.Aggregation
import org.springframework.data.mongodb.core.aggregation.Aggregation.limit
import org.springframework.data.mongodb.core.aggregation.Aggregation.skip
import org.springframework.data.mongodb.core.aggregation.AggregationOperation
import org.springframework.data.mongodb.core.aggregation.AggregationOptions
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toFlux
import reactor.kotlin.core.publisher.toMono
import java.time.LocalDateTime
import java.time.ZoneOffset

@Component
class ProductChangeTimeSeriesCustomRepositoryImpl(
    private val reactiveMongoTemplate: ReactiveMongoTemplate
) : ProductChangeTimeSeriesCustomRepository {

    override fun findProductHistoryBySkuId(
        productId: Long,
        skuId: Long,
        fromTime: LocalDateTime,
        toTime: LocalDateTime,
        limit: Int,
        offset: Int,
    ): Mono<ProductHistorySkuAggregateV2> {
        val fromTime = fromTime.toInstant(ZoneOffset.UTC)
        val toTime = toTime.toInstant(ZoneOffset.UTC)
        val where = Criteria.where("metadata._id.productId").`is`(productId).and("metadata._id.skuId").`is`(skuId)
            .and("timestamp").gte(fromTime).lt(toTime)
        val match = Aggregation.match(where)
        val setWindowFieldsOperation = AggregationOperation {
            Document(
                "\$setWindowFields",
                Document(
                    "partitionBy",
                    Document("id", "\$metadata._id")
                        .append(
                            "day",
                            Document("\$dayOfYear", "\$timestamp")
                        )
                )
                    .append(
                        "sortBy",
                        Document("timestamp", 1L)
                    )
                    .append(
                        "output",
                        Document(
                            "beginOfTotalOrderAmount",
                            Document("\$first", "\$totalOrderAmount")
                                .append(
                                    "window",
                                    Document("range", listOf("unbounded", "unbounded"))
                                        .append("unit", "day")
                                )
                        )
                            .append(
                                "endOfTotalOrderAmount",
                                Document("\$last", "\$totalOrderAmount")
                                    .append(
                                        "window",
                                        Document("range", listOf("unbounded", "unbounded"))
                                            .append("unit", "day")
                                    )
                            )
                            .append(
                                "beginOfDaySkuAmount",
                                Document("\$first", "\$skuAvailableAmount")
                                    .append(
                                        "window",
                                        Document("range", listOf("unbounded", "unbounded"))
                                            .append("unit", "day")
                                    )
                            )
                            .append(
                                "endOfDaySkuAmount",
                                Document("\$last", "\$skuAvailableAmount")
                                    .append(
                                        "window",
                                        Document("range", listOf("unbounded", "unbounded"))
                                            .append("unit", "day")
                                    )
                            )
                            .append(
                                "beginOfDayReviewsAmount",
                                Document("\$first", "\$reviewsAmount")
                                    .append(
                                        "window",
                                        Document("range", listOf("unbounded", "unbounded"))
                                            .append("unit", "day")
                                    )
                            )
                            .append(
                                "endOfDayReviewsAmount",
                                Document("\$last", "\$reviewsAmount")
                                    .append(
                                        "window",
                                        Document("range", listOf("unbounded", "unbounded"))
                                            .append("unit", "day")
                                    )
                            )
                    )
            )
        }
        val setOperation = AggregationOperation {
            Document(
                "\$set",
                Document(
                    "availableAmountDiff",
                    Document("\$subtract", listOf("\$beginOfDaySkuAmount", "\$endOfDaySkuAmount"))
                )
                    .append(
                        "totalOrderAmountDiff",
                        Document("\$subtract", listOf("\$endOfTotalOrderAmount", "\$beginOfTotalOrderAmount"))
                    )
                    .append(
                        "reviewsAmountDiff",
                        Document("\$subtract", listOf("\$endOfDayReviewsAmount", "\$beginOfDayReviewsAmount"))
                    )
            )
        }
        val projectOperation = AggregationOperation {
            Document(
                Document(
                    "\$project",
                    Document("metadata", "\$metadata")
                        .append("timestamp", "\$timestamp")
                        .append("title", "\$title")
                        .append(
                            "price",
                            Document("\$toDouble", "\$price")
                        )
                        .append(
                            "fullPrice",
                            Document("\$toDouble", "\$fullPrice")
                        )
                        .append(
                            "orderAmount",
                            Document(
                                "\$cond",
                                Document(
                                    "if",
                                    Document("\$lt", listOf("\$availableAmountDiff", 0L))
                                )
                                    .append("then", "\$totalOrderAmountDiff")
                                    .append("else", "\$availableAmountDiff")
                            )
                        )
                        .append("totalAvailableAmount", "\$totalAvailableAmount")
                        .append("availableAmount", "\$skuAvailableAmount")
                        .append("reviewsAmount", "\$reviewsAmountDiff")
                        .append("photoKey", "\$photoKey")
                )

            )
        }
        val groupOperation = AggregationOperation {
            Document(
                "\$group",
                Document(
                    "_id",
                    Document(
                        "day",
                        Document(
                            "\$dateTrunc",
                            Document("date", "\$timestamp")
                                .append("unit", "day")
                        )
                    )
                )
                    .append(
                        "productId",
                        Document("\$last", "\$metadata._id.productId")
                    )
                    .append(
                        "skuId",
                        Document("\$last", "\$metadata._id.skuId")
                    )
                    .append(
                        "name",
                        Document("\$last", "\$title")
                    )
                    .append(
                        "reviewsAmount",
                        Document("\$last", "\$reviewsAmount")
                    )
                    .append(
                        "orderAmount",
                        Document("\$last", "\$orderAmount")
                    )
                    .append(
                        "totalAvailableAmount",
                        Document("\$last", "\$totalAvailableAmount")
                    )
                    .append(
                        "availableAmount",
                        Document("\$last", "\$availableAmount")
                    )
                    .append(
                        "fullPrice",
                        Document("\$last", "\$fullPrice")
                    )
                    .append(
                        "price",
                        Document("\$last", "\$price")
                    )
                    .append(
                        "photoKey",
                        Document("\$last", "\$photoKey")
                    )
            )
        }
        val setSalesOperator = AggregationOperation {
            Document(
                "\$set",
                Document(
                    "salesAmount",
                    Document("\$multiply", listOf("\$price", "\$orderAmount"))
                )
            )
        }
        val sortOperation = AggregationOperation {
            Document(
                "\$sort",
                Document("_id.day", 1L)
            )
        }
        val facetOperation = AggregationOperation {
            Document(
                "\$facet",
                Document("metadata", listOf(Document("\$count", "total")))
                    .append(
                        "data", listOf(
                            Document("\$skip", offset),
                            Document("\$limit", limit)
                        )
                    )
            )
        }
        val finalProjection = AggregationOperation {
            Document(
                "\$project",
                Document("data", 1L)
                    .append(
                        "total",
                        Document("\$arrayElemAt", listOf("\$metadata.total", 0L))
                    )
            )
        }
        val aggregation = Aggregation.newAggregation(
            ProductHistorySkuAggregateV2::class.java,
            match,
            setWindowFieldsOperation,
            setOperation,
            projectOperation,
            groupOperation,
            setSalesOperator,
            sortOperation,
            facetOperation,
            finalProjection
        )

        return reactiveMongoTemplate.aggregate(
            aggregation,
            ProductChangeTimeSeries::class.java,
            ProductHistorySkuAggregateV2::class.java
        ).toMono()
    }

    override fun findProductHistoryByProductIds(
        productIds: List<Long>,
        fromTime: LocalDateTime,
        toTime: LocalDateTime
    ): Flux<MultipleProductHistorySalesV2> {
        val fromTime = fromTime.toInstant(ZoneOffset.UTC)
        val toTime = toTime.toInstant(ZoneOffset.UTC)
        val where = Criteria.where("metadata._id.productId").`in`(productIds)
            .and("timestamp").gte(fromTime).lt(toTime)
        val match = Aggregation.match(where)
        val setWindowFieldsOperation = AggregationOperation {
            Document(
                "\$setWindowFields",
                Document(
                    "partitionBy",
                    Document("id", "\$metadata._id")
                        .append(
                            "day",
                            Document("\$dayOfYear", "\$timestamp")
                        )
                )
                    .append(
                        "sortBy",
                        Document("timestamp", 1L)
                    )
                    .append(
                        "output",
                        Document(
                            "beginOfTotalOrderAmount",
                            Document("\$first", "\$totalOrderAmount")
                                .append(
                                    "window",
                                    Document("range", listOf("unbounded", "unbounded"))
                                        .append("unit", "day")
                                )
                        )
                            .append(
                                "endOfTotalOrderAmount",
                                Document("\$last", "\$totalOrderAmount")
                                    .append(
                                        "window",
                                        Document("range", listOf("unbounded", "unbounded"))
                                            .append("unit", "day")
                                    )
                            )
                            .append(
                                "beginOfDaySkuAmount",
                                Document("\$first", "\$skuAvailableAmount")
                                    .append(
                                        "window",
                                        Document("range", listOf("unbounded", "unbounded"))
                                            .append("unit", "day")
                                    )
                            )
                            .append(
                                "endOfDaySkuAmount",
                                Document("\$last", "\$skuAvailableAmount")
                                    .append(
                                        "window",
                                        Document("range", listOf("unbounded", "unbounded"))
                                            .append("unit", "day")
                                    )
                            )
                    )
            )
        }
        val setOperation = AggregationOperation {
            Document(
                "\$set",
                Document(
                    "availableAmountDiff",
                    Document("\$subtract", listOf("\$beginOfDaySkuAmount", "\$endOfDaySkuAmount"))
                )
                    .append(
                        "totalOrderAmountDiff",
                        Document("\$subtract", listOf("\$endOfTotalOrderAmount", "\$beginOfTotalOrderAmount"))
                    )
                    .append(
                        "reviewsAmountDiff",
                        Document("\$subtract", listOf("\$endOfDayReviewsAmount", "\$beginOfDayReviewsAmount"))
                    )
            )
        }
        val projectOperation = AggregationOperation {
            Document(
                "\$project",
                Document("metadata", "\$metadata")
                    .append("timestamp", "\$timestamp")
                    .append("title", "\$title")
                    .append(
                        "price",
                        Document("\$toDouble", "\$price")
                    )
                    .append(
                        "orderAmount",
                        Document(
                            "\$cond",
                            Document(
                                "if",
                                Document("\$lt", listOf("\$availableAmountDiff", 0L))
                            )
                                .append("then", "\$totalOrderAmountDiff")
                                .append("else", "\$availableAmountDiff")
                        )
                    )
                    .append("totalAvailableAmount", "\$totalAvailableAmount")
                    .append("availableAmount", "\$skuAvailableAmount")
            )
        }
        val groupOperation = AggregationOperation {
            Document(
                "\$group",
                Document(
                    "_id",
                    Document("productId", "\$metadata._id.productId")
                        .append("skuId", "\$metadata._id.skuId")
                        .append(
                            "day",
                            Document(
                                "\$dateTrunc",
                                Document("date", "\$timestamp")
                                    .append("unit", "day")
                            )
                        )
                )
                    .append(
                        "sellerTitle",
                        Document("\$last", "\$metadata.sellerTitle")
                    )
                    .append(
                        "sellerLink",
                        Document("\$last", "\$metadata.sellerLink")
                    )
                    .append(
                        "sellerAccountId",
                        Document("\$last", "\$metadata.sellerAccountId")
                    )
                    .append(
                        "orderAmount",
                        Document("\$last", "\$orderAmount")
                    )
                    .append(
                        "price",
                        Document("\$last", "\$price")
                    )
            )
        }
        val setSalesOperator = AggregationOperation {
            Document(
                "\$set",
                Document(
                    "salesAmount",
                    Document("\$multiply", listOf("\$price", "\$orderAmount"))
                )
            )
        }
        val finalGroupOperator = AggregationOperation {
            Document(
                "\$group",
                Document(
                    "_id",
                    Document("productId", "\$_id.productId")
                )
                    .append(
                        "sellerTitle",
                        Document("\$last", "\$sellerTitle")
                    )
                    .append(
                        "sellerLink",
                        Document("\$last", "\$sellerLink")
                    )
                    .append(
                        "sellerAccountId",
                        Document("\$last", "\$sellerAccountId")
                    )
                    .append(
                        "orderAmount",
                        Document("\$sum", "\$orderAmount")
                    )
                    .append(
                        "salesAmount",
                        Document("\$sum", "\$salesAmount")
                    )
                    .append(
                        "dailyOrder",
                        Document("\$avg", "\$orderAmount")
                    )
            )
        }

        val options = AggregationOptions.builder().allowDiskUse(true).build()
        val aggregation = Aggregation.newAggregation(
            ProductHistorySkuAggregateV2::class.java,
            match,
            setWindowFieldsOperation,
            setOperation,
            projectOperation,
            groupOperation,
            setSalesOperator,
            finalGroupOperator,
        ).withOptions(options)

        return reactiveMongoTemplate.aggregate(
            aggregation,
            ProductChangeTimeSeries::class.java,
            MultipleProductHistorySalesV2::class.java
        ).toFlux()
    }

    override fun findProductHistoryBySellers(
        sellerLink: String,
        fromTime: LocalDateTime,
        toTime: LocalDateTime,
        limit: Int,
        offset: Int,
    ): Mono<ProductSellerHistoryAggregateWrapper> {
        val fromTime = fromTime.toInstant(ZoneOffset.UTC)
        val toTime = toTime.toInstant(ZoneOffset.UTC)
        val where = Criteria.where("metadata.sellerLink").`is`(sellerLink)
            .and("timestamp").gte(fromTime).lt(toTime)
        val match = Aggregation.match(where)
        val setWindowFieldsOperation = AggregationOperation {
            Document(
                "\$setWindowFields",
                Document(
                    "partitionBy",
                    Document("id", "\$metadata._id")
                        .append(
                            "day",
                            Document("\$dayOfYear", "\$timestamp")
                        )
                )
                    .append(
                        "sortBy",
                        Document("timestamp", 1L)
                    )
                    .append(
                        "output",
                        Document(
                            "beginOfTotalOrderAmount",
                            Document("\$first", "\$totalOrderAmount")
                                .append(
                                    "window",
                                    Document("range", listOf("unbounded", "unbounded"))
                                        .append("unit", "day")
                                )
                        )
                            .append(
                                "endOfTotalOrderAmount",
                                Document("\$last", "\$totalOrderAmount")
                                    .append(
                                        "window",
                                        Document("range", listOf("unbounded", "unbounded"))
                                            .append("unit", "day")
                                    )
                            )
                            .append(
                                "beginOfDaySkuAmount",
                                Document("\$first", "\$skuAvailableAmount")
                                    .append(
                                        "window",
                                        Document("range", listOf("unbounded", "unbounded"))
                                            .append("unit", "day")
                                    )
                            )
                            .append(
                                "endOfDaySkuAmount",
                                Document("\$last", "\$skuAvailableAmount")
                                    .append(
                                        "window",
                                        Document("range", listOf("unbounded", "unbounded"))
                                            .append("unit", "day")
                                    )
                            )
                            .append(
                                "beginOfDayReviewsAmount",
                                Document("\$first", "\$reviewsAmount")
                                    .append(
                                        "window",
                                        Document("range", listOf("unbounded", "unbounded"))
                                            .append("unit", "day")
                                    )
                            )
                            .append(
                                "endOfDayReviewsAmount",
                                Document("\$last", "\$reviewsAmount")
                                    .append(
                                        "window",
                                        Document("range", listOf("unbounded", "unbounded"))
                                            .append("unit", "day")
                                    )
                            )
                    )
            )
        }
        val setOperation = AggregationOperation {
            Document(
                "\$set",
                Document(
                    "availableAmountDiff",
                    Document("\$subtract", listOf("\$beginOfDaySkuAmount", "\$endOfDaySkuAmount"))
                )
                    .append(
                        "totalOrderAmountDiff",
                        Document("\$subtract", listOf("\$endOfTotalOrderAmount", "\$beginOfTotalOrderAmount"))
                    )
                    .append(
                        "reviewsAmountDiff",
                        Document("\$subtract", listOf("\$endOfDayReviewsAmount", "\$beginOfDayReviewsAmount"))
                    )
            )
        }
        val projectOperation = AggregationOperation {
            Document(
                "\$project",
                Document("metadata", "\$metadata")
                    .append("timestamp", "\$timestamp")
                    .append("title", "\$title")
                    .append(
                        "price",
                        Document("\$toDouble", "\$price")
                    )
                    .append(
                        "fullPrice",
                        Document("\$toDouble", "\$fullPrice")
                    )
                    .append(
                        "orderAmount",
                        Document(
                            "\$cond",
                            Document(
                                "if",
                                Document("\$lt", listOf("\$availableAmountDiff", 0L))
                            )
                                .append("then", "\$totalOrderAmountDiff")
                                .append("else", "\$availableAmountDiff")
                        )
                    )
                    .append("totalAvailableAmount", "\$totalAvailableAmount")
                    .append("availableAmount", "\$skuAvailableAmount")
                    .append("reviewsAmount", "\$reviewsAmountDiff")
            )
        }
        val firstGroupOperation = AggregationOperation {
            Document(
                "\$group",
                Document(
                    "_id",
                    Document(
                        "timestamp",
                        Document(
                            "\$dateTrunc",
                            Document("date", "\$timestamp")
                                .append("unit", "day")
                        )
                    )
                        .append("productId", "\$metadata._id.productId")
                        .append("skuId", "\$metadata._id.skuId")
                )
                    .append(
                        "name",
                        Document("\$last", "\$title")
                    )
                    .append(
                        "sellerId",
                        Document("\$last", "\$metadata.sellerId")
                    )
                    .append(
                        "sellerTitle",
                        Document("\$last", "\$metadata.sellerTitle")
                    )
                    .append(
                        "categoryName",
                        Document("\$last", "\$metadata.category")
                    )
                    .append(
                        "orderAmount",
                        Document("\$last", "\$orderAmount")
                    )
                    .append(
                        "availableAmount",
                        Document("\$last", "\$availableAmount")
                    )
                    .append(
                        "price",
                        Document("\$last", "\$price")
                    )
            )
        }
        val proceedsSetOperation = AggregationOperation {
            Document(
                "\$set",
                Document(
                    "proceeds",
                    Document("\$multiply", listOf("\$price", "\$orderAmount"))
                )
            )
        }
        val sortOperation = AggregationOperation {
            Document(
                "\$sort",
                Document("_id.timestamp", 1L)
            )
        }
        val secondGroup = AggregationOperation {
            Document(
                "\$group",
                Document(
                    "_id",
                    Document("productId", "\$_id.productId")
                        .append("skuId", "\$_id.skuId")
                )
                    .append(
                        "name",
                        Document("\$last", "\$name")
                    )
                    .append(
                        "sellerId",
                        Document("\$last", "\$sellerId")
                    )
                    .append(
                        "sellerTitle",
                        Document("\$last", "\$sellerTitle")
                    )
                    .append(
                        "categoryName",
                        Document("\$last", "\$categoryName")
                    )
                    .append(
                        "orderAmountGraph",
                        Document(
                            "\$push",
                            Document("orderAmount", "\$orderAmount")
                                .append(
                                    "time",
                                    Document(
                                        "\$dateToString",
                                        Document("format", "%d-%m-%Y")
                                            .append("date", "\$_id.timestamp")
                                    )
                                )
                        )
                    )
                    .append(
                        "availableAmountGraph",
                        Document(
                            "\$push",
                            Document("availableAmount", "\$availableAmount")
                                .append(
                                    "time",
                                    Document(
                                        "\$dateToString",
                                        Document("format", "%d-%m-%Y")
                                            .append("date", "\$_id.timestamp")
                                    )
                                )
                        )
                    )
                    .append(
                        "availableAmount",
                        Document("\$last", "\$availableAmount")
                    )
                    .append(
                        "priceGraph",
                        Document(
                            "\$push",
                            Document("price", "\$price")
                                .append(
                                    "time",
                                    Document(
                                        "\$dateToString",
                                        Document("format", "%d-%m-%Y")
                                            .append("date", "\$_id.timestamp")
                                    )
                                )
                        )
                    )
                    .append(
                        "price",
                        Document("\$last", "\$price")
                    ).append(
                        "proceeds",
                        Document("\$sum", "\$proceeds")
                    )
            )
        }
        val finalSort = AggregationOperation {
            Document(
                "\$sort",
                Document("_id", 1L)
            )
        }
        val faceitOperation = AggregationOperation {
            Document(
                "\$facet",
                Document("metadata", listOf(Document("\$count", "total")))
                    .append(
                        "data", listOf(
                            Document("\$skip", offset),
                            Document("\$limit", limit)
                        )
                    )
            )
        }
        val finalProjectOperation = AggregationOperation {
            Document(
                "\$project",
                Document("data", 1L)
                    .append(
                        "total",
                        Document("\$arrayElemAt", listOf("\$metadata.total", 0L))
                    )
            )
        }

        val options = AggregationOptions.builder().allowDiskUse(true).build()
        val aggregation = Aggregation.newAggregation(
            ProductHistorySkuAggregateV2::class.java,
            match,
            setWindowFieldsOperation,
            setOperation,
            projectOperation,
            firstGroupOperation,
            proceedsSetOperation,
            sortOperation,
            secondGroup,
            finalSort,
            faceitOperation,
            finalProjectOperation
        ).withOptions(options)

        return reactiveMongoTemplate.aggregate(
            aggregation,
            ProductChangeTimeSeries::class.java,
            ProductSellerHistoryAggregateWrapper::class.java
        ).toMono()
    }

    override fun findProductHistoryByCategory(
        categoryPath: List<String>,
        fromTime: LocalDateTime,
        toTime: LocalDateTime,
        limit: Int,
        offset: Int
    ): Mono<MutableList<ProductSellerHistoryAggregate>> {
        val regexCategoryPath = "^," + categoryPath.joinToString(separator = ",") + ","
        val fromTimeUtc = fromTime.toInstant(ZoneOffset.UTC)
        val endTimeUtc = toTime.toInstant(ZoneOffset.UTC)
        val match = Aggregation.match(
            Criteria.where("metadata.categoryPath").regex(regexCategoryPath)
                .and("timestamp").gte(fromTimeUtc).lt(endTimeUtc)
        )
        val setWindowFieldsOperation = AggregationOperation {
            Document(
                "\$setWindowFields",
                Document(
                    "partitionBy",
                    Document("id", "\$metadata._id")
                        .append(
                            "day",
                            Document("\$dayOfYear", "\$timestamp")
                        )
                )
                    .append(
                        "sortBy",
                        Document("timestamp", 1L)
                    )
                    .append(
                        "output",
                        Document(
                            "beginOfTotalOrderAmount",
                            Document("\$first", "\$totalOrderAmount")
                                .append(
                                    "window",
                                    Document("range", listOf("unbounded", "unbounded"))
                                        .append("unit", "day")
                                )
                        )
                            .append(
                                "endOfTotalOrderAmount",
                                Document("\$last", "\$totalOrderAmount")
                                    .append(
                                        "window",
                                        Document("range", listOf("unbounded", "unbounded"))
                                            .append("unit", "day")
                                    )
                            )
                            .append(
                                "beginOfDaySkuAmount",
                                Document("\$first", "\$skuAvailableAmount")
                                    .append(
                                        "window",
                                        Document("range", listOf("unbounded", "unbounded"))
                                            .append("unit", "day")
                                    )
                            )
                            .append(
                                "endOfDaySkuAmount",
                                Document("\$last", "\$skuAvailableAmount")
                                    .append(
                                        "window",
                                        Document("range", listOf("unbounded", "unbounded"))
                                            .append("unit", "day")
                                    )
                            )
                            .append(
                                "beginOfDayReviewsAmount",
                                Document("\$first", "\$reviewsAmount")
                                    .append(
                                        "window",
                                        Document("range", listOf("unbounded", "unbounded"))
                                            .append("unit", "day")
                                    )
                            )
                            .append(
                                "endOfDayReviewsAmount",
                                Document("\$last", "\$reviewsAmount")
                                    .append(
                                        "window",
                                        Document("range", listOf("unbounded", "unbounded"))
                                            .append("unit", "day")
                                    )
                            )
                    )
            )
        }
        val setOperation = AggregationOperation {
            Document(
                "\$set",
                Document(
                    "availableAmountDiff",
                    Document("\$subtract", listOf("\$beginOfDaySkuAmount", "\$endOfDaySkuAmount"))
                )
                    .append(
                        "totalOrderAmountDiff",
                        Document("\$subtract", listOf("\$endOfTotalOrderAmount", "\$beginOfTotalOrderAmount"))
                    )
                    .append(
                        "reviewsAmountDiff",
                        Document("\$subtract", listOf("\$endOfDayReviewsAmount", "\$beginOfDayReviewsAmount"))
                    )
            )
        }
        val projectOperation = AggregationOperation {
            Document(
                "\$project",
                Document("metadata", "\$metadata")
                    .append("timestamp", "\$timestamp")
                    .append("title", "\$title")
                    .append(
                        "price",
                        Document("\$toDouble", "\$price")
                    )
                    .append(
                        "fullPrice",
                        Document("\$toDouble", "\$fullPrice")
                    )
                    .append(
                        "orderAmount",
                        Document(
                            "\$cond",
                            Document(
                                "if",
                                Document("\$lt", listOf("\$availableAmountDiff", 0L))
                            )
                                .append("then", "\$totalOrderAmountDiff")
                                .append("else", "\$availableAmountDiff")
                        )
                    )
                    .append("totalAvailableAmount", "\$totalAvailableAmount")
                    .append("availableAmount", "\$skuAvailableAmount")
                    .append("reviewsAmount", "\$reviewsAmountDiff")
            )
        }
        val firstGroupOperation = AggregationOperation {
            Document(
                "\$group",
                Document(
                    "_id",
                    Document(
                        "timestamp",
                        Document(
                            "\$dateTrunc",
                            Document("date", "\$timestamp")
                                .append("unit", "day")
                        )
                    )
                        .append("productId", "\$metadata._id.productId")
                        .append("skuId", "\$metadata._id.skuId")
                )
                    .append(
                        "name",
                        Document("\$last", "\$title")
                    )
                    .append(
                        "sellerId",
                        Document("\$last", "\$metadata.sellerId")
                    )
                    .append(
                        "sellerTitle",
                        Document("\$last", "\$metadata.sellerTitle")
                    )
                    .append(
                        "categoryName",
                        Document("\$last", "\$metadata.category")
                    )
                    .append(
                        "orderAmount",
                        Document("\$last", "\$orderAmount")
                    )
                    .append(
                        "availableAmount",
                        Document("\$last", "\$availableAmount")
                    )
                    .append(
                        "price",
                        Document("\$last", "\$price")
                    )
            )
        }
        val proceedsSetOperation = AggregationOperation {
            Document(
                "\$set",
                Document(
                    "proceeds",
                    Document("\$multiply", listOf("\$price", "\$orderAmount"))
                )
            )
        }
        val sortOperation = AggregationOperation {
            Document(
                "\$sort",
                Document("_id.timestamp", 1L)
            )
        }
        val secondGroup = AggregationOperation {
            Document(
                "\$group",
                Document(
                    "_id",
                    Document("productId", "\$_id.productId")
                        .append("skuId", "\$_id.skuId")
                )
                    .append(
                        "name",
                        Document("\$last", "\$name")
                    )
                    .append(
                        "sellerId",
                        Document("\$last", "\$sellerId")
                    )
                    .append(
                        "sellerTitle",
                        Document("\$last", "\$sellerTitle")
                    )
                    .append(
                        "categoryName",
                        Document("\$last", "\$categoryName")
                    )
                    .append(
                        "orderAmountGraph",
                        Document(
                            "\$push",
                            Document("orderAmount", "\$orderAmount")
                                .append(
                                    "time",
                                    Document(
                                        "\$dateToString",
                                        Document("format", "%d-%m-%Y")
                                            .append("date", "\$_id.timestamp")
                                    )
                                )
                        )
                    )
                    .append(
                        "availableAmountGraph",
                        Document(
                            "\$push",
                            Document("availableAmount", "\$availableAmount")
                                .append(
                                    "time",
                                    Document(
                                        "\$dateToString",
                                        Document("format", "%d-%m-%Y")
                                            .append("date", "\$_id.timestamp")
                                    )
                                )
                        )
                    )
                    .append(
                        "availableAmount",
                        Document("\$last", "\$availableAmount")
                    )
                    .append(
                        "priceGraph",
                        Document(
                            "\$push",
                            Document("price", "\$price")
                                .append(
                                    "time",
                                    Document(
                                        "\$dateToString",
                                        Document("format", "%d-%m-%Y")
                                            .append("date", "\$_id.timestamp")
                                    )
                                )
                        )
                    )
                    .append(
                        "price",
                        Document("\$last", "\$price")
                    ).append(
                        "proceeds",
                        Document("\$sum", "\$proceeds")
                    )
            )
        }

        val options = AggregationOptions.builder().allowDiskUse(true).build()
        val aggregation = Aggregation.newAggregation(
            ProductHistorySkuAggregateV2::class.java,
            listOf(
                match, setWindowFieldsOperation, setOperation, projectOperation,
                firstGroupOperation, proceedsSetOperation, sortOperation,
                secondGroup, skip(offset.toLong()), limit(limit.toLong() + 1)
            )
        ).withOptions(options)

        return reactiveMongoTemplate.aggregate(
            aggregation,
            ProductChangeTimeSeries::class.java,
            ProductSellerHistoryAggregate::class.java
        ).collectList().toMono()
    }
}
