package dev.crashteam.keanalytics.repository.mongo

import com.mongodb.client.result.UpdateResult
import dev.crashteam.keanalytics.domain.mongo.ReferralCodeDocument
import dev.crashteam.keanalytics.domain.mongo.ReferralInvitedUserDocument
import org.springframework.data.mongodb.core.ReactiveMongoTemplate
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.Update
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono
import java.time.LocalDateTime

@Component
class ReferralCodeCustomRepositoryImpl(
    private val reactiveMongoTemplate: ReactiveMongoTemplate,
) : ReferralCodeCustomRepository {

    override fun addReferralCodeUser(referralCode: String, userId: String, subscriptionName: String): Mono<UpdateResult> {
        val query = Query().apply { addCriteria(Criteria.where("code").`is`(referralCode)) }
        val update = Update().apply {
            push("invited", ReferralInvitedUserDocument(userId, LocalDateTime.now(), subscriptionName))
        }

        return reactiveMongoTemplate.upsert(query, update, ReferralCodeDocument::class.java)
    }

    override fun findReferralInvitedUser(userId: String): Mono<ReferralCodeDocument> {
        val query = Query().apply { addCriteria(Criteria.where("invited.userId").`is`(userId)) }

        return reactiveMongoTemplate.findOne(query, ReferralCodeDocument::class.java)
    }
}
