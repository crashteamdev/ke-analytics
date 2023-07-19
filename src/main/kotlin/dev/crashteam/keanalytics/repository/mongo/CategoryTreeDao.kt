package dev.crashteam.keanalytics.repository.mongo

import com.mongodb.client.result.UpdateResult
import dev.crashteam.keanalytics.domain.mongo.CategoryTreeDocument
import org.springframework.data.mongodb.core.ReactiveMongoTemplate
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.Update
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono

@Component
class CategoryTreeDao(
    private val reactiveMongoTemplate: ReactiveMongoTemplate,
) {

    fun saveCategory(categoryTreeDocument: CategoryTreeDocument): Mono<UpdateResult> {
        val query = Query().apply { addCriteria(Criteria.where("categoryId").`is`(categoryTreeDocument.categoryId)) }
        val update = Update().apply {
            set("categoryId", categoryTreeDocument.categoryId)
            set("parentCategoryId", categoryTreeDocument.parentCategoryId)
            set("title", categoryTreeDocument.title)
        }

        return reactiveMongoTemplate.upsert(query, update, CategoryTreeDocument::class.java)
    }
}
