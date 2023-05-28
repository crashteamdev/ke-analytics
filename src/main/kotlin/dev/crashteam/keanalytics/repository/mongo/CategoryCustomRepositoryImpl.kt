package dev.crashteam.keanalytics.repository.mongo

import dev.crashteam.keanalytics.domain.mongo.ProductChangeTimeSeries
import dev.crashteam.keanalytics.repository.mongo.model.CategoryAveragePriceAggregate
import org.springframework.data.domain.Sort
import org.springframework.data.mongodb.core.ReactiveMongoTemplate
import org.springframework.data.mongodb.core.aggregation.*
import org.springframework.data.mongodb.core.aggregation.ArithmeticOperators.Divide
import org.springframework.data.mongodb.core.aggregation.ArithmeticOperators.Subtract
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toMono
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZoneOffset
import java.util.regex.Pattern

@Component
class CategoryCustomRepositoryImpl(
    private val reactiveMongoTemplate: ReactiveMongoTemplate,
) : CategoryCustomRepository {

    override fun aggregateCategoryAveragePrice(
        categoryPath: List<String>,
        date: LocalDate
    ): Mono<CategoryAveragePriceAggregate> {
        val regexCategoryPath = "^," + categoryPath.joinToString(separator = ",") + ","
        val startOfDay = date.atStartOfDay().toInstant(ZoneOffset.UTC)
        val endOfDay = date.atTime(LocalTime.MAX).toInstant(ZoneOffset.UTC)
        val match = Aggregation.match(
            Criteria.where("metadata.categoryPath").regex(regexCategoryPath)
                .and("timestamp").gte(startOfDay).lt(endOfDay)
        )
        val project = Aggregation.project("metadata", "timestamp", "totalOrderAmount", "price")
            .and(ConvertOperators.ToDouble.toDouble("\$price")).`as`("price")
        val sort = Aggregation.sort(Sort.Direction.ASC, "timestamp")
        val group = Aggregation.group("metadata._id.productId")
            .last("price").`as`("price")
            .first("totalOrderAmount").`as`("beginOfDayTotalOrderAmount")
            .last("totalOrderAmount").`as`("endOfDayTotalOrderAmount")
        val addFields = Aggregation.addFields().addField("totalOrderAmountDifference")
            .withValue(
                ArithmeticOperators.Abs.absoluteValueOf(
                    Subtract.valueOf("endOfDayTotalOrderAmount").subtract("beginOfDayTotalOrderAmount")
                )
            ).build()
        val matchDifference = Aggregation.match(
            Criteria.where("totalOrderAmountDifference").gt(0)
        )
        val groupPrice = Aggregation.group()
            .sum("price").`as`("overallPrice")
            .count().`as`("productCount")
        val finalProject = Aggregation.project("productCount")
            .and(
                ArithmeticOperators.Abs.absoluteValueOf(
                    Divide.valueOf("overallPrice").divideBy("productCount")
                )
            ).`as`("averagePrice")

        val options = AggregationOptions.builder().allowDiskUse(true).build()
        val aggr: TypedAggregation<CategoryAveragePriceAggregate> =
            Aggregation.newAggregation(
                CategoryAveragePriceAggregate::class.java,
                listOf(match, project, sort, group, addFields, matchDifference, groupPrice, finalProject)
            ).withOptions(options)

        return reactiveMongoTemplate.aggregate(
            aggr,
            ProductChangeTimeSeries::class.java,
            CategoryAveragePriceAggregate::class.java
        ).toMono()
    }
}
