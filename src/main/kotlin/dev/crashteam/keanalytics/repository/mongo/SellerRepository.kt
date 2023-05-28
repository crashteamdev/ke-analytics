package dev.crashteam.keanalytics.repository.mongo

import dev.crashteam.keanalytics.domain.mongo.SellerDetailDocument
import dev.crashteam.keanalytics.domain.mongo.UserDocument
import org.springframework.data.repository.reactive.ReactiveCrudRepository
import org.springframework.stereotype.Repository
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.LocalDateTime

@Repository
interface SellerRepository : ReactiveCrudRepository<SellerDetailDocument, String>, SellerCustomRepository {

    fun findByAccountId(accountId: Long): Flux<SellerDetailDocument>

    fun findByLink(sellerLink: String): Mono<SellerDetailDocument>
}
