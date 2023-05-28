package dev.crashteam.keanalytics.repository.mongo

import dev.crashteam.keanalytics.domain.mongo.ReferralCodeDocument
import org.springframework.data.mongodb.repository.ReactiveMongoRepository
import org.springframework.stereotype.Repository
import reactor.core.publisher.Mono

@Repository
interface ReferralCodeRepository : ReactiveMongoRepository<ReferralCodeDocument, String>, ReferralCodeCustomRepository {

    fun findByUserId(userId: String): Mono<ReferralCodeDocument>

    fun findByCode(code: String): Mono<ReferralCodeDocument>
}
