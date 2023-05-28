package dev.crashteam.keanalytics.repository.mongo

import dev.crashteam.keanalytics.domain.mongo.UserDocument
import org.springframework.data.repository.reactive.ReactiveCrudRepository
import org.springframework.stereotype.Repository
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.LocalDateTime

@Repository
interface UserRepository : ReactiveCrudRepository<UserDocument, String> {

    fun findByUserId(userId: String): Mono<UserDocument>

    fun findByEmail(email: String): Flux<UserDocument>

    fun findByApiKey_HashKey(hashKey: String): Mono<UserDocument>

    fun findBySubscriptionEndAtBefore(date: LocalDateTime): Flux<UserDocument>
}
