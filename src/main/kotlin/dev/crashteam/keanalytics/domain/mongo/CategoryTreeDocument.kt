package dev.crashteam.keanalytics.domain.mongo

import org.bson.types.ObjectId
import org.springframework.data.mongodb.core.mapping.Document
import org.springframework.data.mongodb.core.mapping.MongoId

@Document("categories_hierarchical")
data class CategoryTreeDocument(
    val categoryId: Long,
    val parentCategoryId: Long,
    val title: String,

    @MongoId
    val id: ObjectId = ObjectId(),
)
