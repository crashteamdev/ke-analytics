package dev.crashteam.keanalytics.repository.mongo

import dev.crashteam.keanalytics.domain.mongo.CategoryTreeDocument
import org.springframework.data.repository.reactive.ReactiveCrudRepository
import org.springframework.stereotype.Repository

@Repository
interface CategoryTreeRepository : ReactiveCrudRepository<CategoryTreeDocument, String> {
}
