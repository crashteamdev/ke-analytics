package dev.crashteam.keanalytics.repository.mongo

import com.mongodb.client.result.UpdateResult
import dev.crashteam.keanalytics.domain.mongo.CategoryDocument
import org.springframework.data.mongodb.core.ReactiveMongoTemplate
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.Update
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono

@Component
class CategoryDao(
    private val reactiveMongoTemplate: ReactiveMongoTemplate,
) {

    fun saveCategory(categoryDocument: CategoryDocument): Mono<UpdateResult> {
        val query = Query().apply { addCriteria(Criteria.where("publicId").`is`(categoryDocument.publicId)) }
        val update = Update().apply {
            set("publicId", categoryDocument.publicId)
            set("productAmount", categoryDocument.productAmount)
            set("adult", categoryDocument.adult)
            set("eco", categoryDocument.eco)
            set("title", categoryDocument.title)
            set("path", categoryDocument.path)
            set("updatedAt", categoryDocument.updatedAt)
        }

        return reactiveMongoTemplate.upsert(query, update, CategoryDocument::class.java)
    }
}
