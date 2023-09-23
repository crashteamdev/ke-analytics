package dev.crashteam.keanalytics.repository.mongo

import dev.crashteam.keanalytics.domain.mongo.PromoCodeDocument
import org.springframework.data.repository.reactive.ReactiveCrudRepository
import org.springframework.stereotype.Repository
import reactor.core.publisher.Mono

@Repository
interface PromoCodeRepository : ReactiveCrudRepository<PromoCodeDocument, String> {

    fun findByCode(code: String): Mono<PromoCodeDocument>
}
