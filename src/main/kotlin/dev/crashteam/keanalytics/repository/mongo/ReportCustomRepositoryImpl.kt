package dev.crashteam.keanalytics.repository.mongo

import com.mongodb.client.result.UpdateResult
import dev.crashteam.keanalytics.domain.mongo.ReportDocument
import dev.crashteam.keanalytics.domain.mongo.ReportStatus
import org.springframework.data.mongodb.core.ReactiveMongoTemplate
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.Update
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.*
import java.util.*

@Component
class ReportCustomRepositoryImpl(
    private val reactiveMongoTemplate: ReactiveMongoTemplate
) : ReportCustomRepository {

    override fun updateReportStatus(jobId: String, reportStatus: ReportStatus): Mono<UpdateResult> {
        val query = Query().apply { addCriteria(Criteria.where("jobId").`is`(jobId)) }
        val update = Update().apply {
            set("status", reportStatus)
        }
        return reactiveMongoTemplate.updateFirst(query, update, ReportDocument::class.java)
    }

    override fun setReportId(jobId: String, reportId: String): Mono<UpdateResult> {
        val query = Query().apply { addCriteria(Criteria.where("jobId").`is`(jobId)) }
        val update = Update().apply {
            set("reportId", reportId)
        }
        return reactiveMongoTemplate.updateFirst(query, update, ReportDocument::class.java)
    }

    override fun findTodayReportBySellerLink(sellerLink: String, userId: String): Flux<ReportDocument> {
        val query = Query().apply {
            val startOfDay: Instant = LocalDate.now(ZoneId.of("UTC")).atTime(LocalTime.MIN).toInstant(ZoneOffset.UTC)
            val endOfDay: Instant = LocalDate.now(ZoneId.of("UTC")).atTime(LocalTime.MAX).toInstant(ZoneOffset.UTC)
            addCriteria(Criteria.where("sellerLink").`is`(sellerLink)
                .and("userId").`is`(userId)
                .and("createdAt").gte(startOfDay).lt(endOfDay))
        }
        return reactiveMongoTemplate.find(query, ReportDocument::class.java)
    }

    override fun findByUserIdAndCreatedAtFromTime(
        userId: String,
        fromTime: LocalDateTime,
    ): Flux<ReportDocument> {
        val query = Query().apply {
            val fromDateTime = Date.from(fromTime.toInstant(ZoneOffset.UTC))
            addCriteria(
                Criteria.where("userId").`is`(userId)
                    .andOperator(
                        Criteria.where("createdAt").gte(fromDateTime)
                    )
            )
        }
        return reactiveMongoTemplate.find(query, ReportDocument::class.java)
    }


}
