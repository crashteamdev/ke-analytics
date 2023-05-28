package dev.crashteam.keanalytics.repository.mongo

import dev.crashteam.keanalytics.domain.mongo.ReportDocument
import dev.crashteam.keanalytics.domain.mongo.ReportStatus
import org.springframework.data.repository.reactive.ReactiveCrudRepository
import org.springframework.stereotype.Repository
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

@Repository
interface ReportRepository : ReactiveCrudRepository<ReportDocument, String>, ReportCustomRepository {

    fun findByReportId(reportId: String): Mono<ReportDocument>

    fun findByJobId(jobId: String): Mono<ReportDocument>

    fun deleteByReportId(reportId: String)

    fun deleteByJobId(jobId: String)

    fun findAllByStatus(reportStatus: ReportStatus): Flux<ReportDocument>

    fun findByRequestIdAndSellerLink(requestId: String, sellerLink: String): Mono<ReportDocument>

    fun findByRequestIdAndCategoryPublicId(requestId: String, categoryPublicId: Long): Mono<ReportDocument>

}
