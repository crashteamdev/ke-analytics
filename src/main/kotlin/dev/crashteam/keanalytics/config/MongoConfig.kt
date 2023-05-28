package dev.crashteam.keanalytics.config

import kotlinx.coroutines.reactive.awaitSingle
import kotlinx.coroutines.runBlocking
import dev.crashteam.keanalytics.domain.mongo.ProductChangeTimeSeries
import dev.crashteam.keanalytics.domain.mongo.ProductPositionTSDocument
import org.bson.Document
import org.springframework.context.annotation.Configuration
import org.springframework.data.mongodb.core.ReactiveMongoTemplate
import org.springframework.data.mongodb.core.index.CompoundIndexDefinition
import javax.annotation.PostConstruct

@Configuration
class MongoConfig(
    private val reactiveMongoTemplate: ReactiveMongoTemplate
) {

    @PostConstruct
    fun init() = runBlocking {
        val productPositionCollectionExists =
            reactiveMongoTemplate.collectionExists(ProductPositionTSDocument::class.java).awaitSingle()
        if (!productPositionCollectionExists) {
            reactiveMongoTemplate.createCollection(ProductPositionTSDocument::class.java).awaitSingle()
            reactiveMongoTemplate.indexOps(ProductPositionTSDocument::class.java)
                .ensureIndex(
                    CompoundIndexDefinition(
                        Document().append("metadata.categoryId", 1)
                            .append("metadata.id.productId", 1)
                            .append("metadata.id.skuId", 1)
                    )
                ).awaitSingle()
        }
        val productChangeTsExists = reactiveMongoTemplate.collectionExists(ProductChangeTimeSeries::class.java).awaitSingle()
        if (!productChangeTsExists) {
            reactiveMongoTemplate.createCollection(ProductChangeTimeSeries::class.java).awaitSingle()
            reactiveMongoTemplate.indexOps(ProductChangeTimeSeries::class.java)
                .ensureIndex(
                    CompoundIndexDefinition(
                        Document().append("metadata.parentCategory", 1)
                            .append("metadata.id.productId", 1)
                            .append("metadata.id.skuId", 1)
                    )
                ).awaitSingle()

        }
    }

}
