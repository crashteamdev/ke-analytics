package dev.crashteam.keanalytics.repository.mongo

import dev.crashteam.keanalytics.domain.mongo.ProductPositionTSDocument
import dev.crashteam.keanalytics.repository.mongo.model.ProductPositionAggregate
import org.springframework.data.domain.Sort
import org.springframework.data.mongodb.core.ReactiveMongoTemplate
import org.springframework.data.mongodb.core.aggregation.Aggregation
import org.springframework.data.mongodb.core.aggregation.TypedAggregation
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux
import java.time.LocalDateTime

@Component
class ProductPositionRepositoryImpl(
    private val reactiveMongoTemplate: ReactiveMongoTemplate,
) : ProductPositionCustomRepository {

    override fun findProductPositions(
        categoryId: Long,
        productId: Long,
        skuId: Long,
        fromTime: LocalDateTime,
        toTime: LocalDateTime
    ): Flux<ProductPositionAggregate> {
        val match = Aggregation.match(
            Criteria.where("timestamp").gt(fromTime).andOperator(
                Criteria.where("timestamp").lt(toTime)
            ).and("metadata.categoryId").`is`(categoryId)
                .and("metadata._id.productId").`is`(productId)
                .and("metadata._id.skuId").`is`(skuId)
        )
        val project = Aggregation.project()
            .and("metadata._id.productId").`as`("productId")
            .and("metadata._id.skuId").`as`("skuId")
            .and("metadata.categoryId").`as`("categoryId")
            .and("position").`as`("position")
            .andExpression("timestamp").dateAsFormattedString("%Y-%m-%d").`as`("date")
        val group = Aggregation.group("date", "productId", "skuId", "categoryId")
            .last("position").`as`("position")
        val sort = Aggregation.sort(Sort.Direction.ASC, "date")
        val aggr: TypedAggregation<ProductPositionAggregate> =
            Aggregation.newAggregation(
                ProductPositionAggregate::class.java,
                listOf(match, project, group, sort)
            )

        return reactiveMongoTemplate.aggregate(
            aggr,
            ProductPositionTSDocument::class.java,
            ProductPositionAggregate::class.java
        )
    }
}

