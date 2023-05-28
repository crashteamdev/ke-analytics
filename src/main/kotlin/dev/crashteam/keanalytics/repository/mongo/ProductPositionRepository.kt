package dev.crashteam.keanalytics.repository.mongo

import dev.crashteam.keanalytics.domain.mongo.ProductPositionTSDocument
import org.springframework.data.repository.reactive.ReactiveCrudRepository
import org.springframework.stereotype.Repository

@Repository
interface ProductPositionRepository : ReactiveCrudRepository<ProductPositionTSDocument, String>,
    ProductPositionCustomRepository

