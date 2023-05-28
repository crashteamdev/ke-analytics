package dev.crashteam.keanalytics.domain.mongo

import org.bson.types.ObjectId
import org.springframework.data.mongodb.core.index.Indexed
import org.springframework.data.mongodb.core.mapping.Document
import org.springframework.data.mongodb.core.mapping.MongoId
import java.time.LocalDateTime

@Document("categories")
data class CategoryDocument(

    @Indexed(unique = true)
    val publicId: Long,

    val productAmount: Long?,

    val adult: Boolean,

    val eco: Boolean,

    val title: String,

    val path: String?,

    val updatedAt: LocalDateTime,

    @MongoId
    val id: ObjectId = ObjectId(),

    @Transient
    val document: HashSet<CategoryDocument> = HashSet()
)
