package dev.crashteam.keanalytics.repository.mongo

import dev.crashteam.keanalytics.domain.mongo.CategoryDocument
import org.springframework.data.mongodb.repository.Query
import org.springframework.data.repository.reactive.ReactiveCrudRepository
import org.springframework.stereotype.Repository
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

@Repository
interface CategoryRepository : ReactiveCrudRepository<CategoryDocument, String>, CategoryCustomRepository {

    fun findByPublicId(publicId: Long): Mono<CategoryDocument>

    fun findByPublicIdIn(publicIds: List<Long>): Flux<CategoryDocument>

    fun findByPathIsNull(): Flux<CategoryDocument>

    @Query("{'path':{'\$regex':'?0','\$options':'i'}}")
    fun findByPath(path: String): Flux<CategoryDocument>

    fun findByTitle(title: String): Mono<CategoryDocument>

    fun findAllByOrderByPath(): Flux<CategoryDocument>

    @Query("{'path':{'\$regex':'?0','\$options':'i'}}")
    fun findByPathOrderByPath(path: String): Flux<CategoryDocument>
}
