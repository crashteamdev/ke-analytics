package dev.crashteam.keanalytics.report

import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactor.awaitSingleOrNull
import dev.crashteam.keanalytics.domain.mongo.ReportStatus
import dev.crashteam.keanalytics.domain.mongo.ReportType
import dev.crashteam.keanalytics.repository.mongo.ReportRepository
import org.springframework.data.redis.core.ReactiveRedisCallback
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.io.InputStream
import java.nio.ByteBuffer
import java.time.Duration
import java.time.LocalDate
import java.time.LocalTime


@Service
class ReportService(
    private val reportFileService: ReportFileService,
    private val reportRepository: ReportRepository,
) {

    @Transactional
    suspend fun saveSellerReportV2(sellerLink: String, interval: Int, jobId: String, reportInputStream: InputStream) {
        val reportId: String =
            reportFileService.saveSellerReport(sellerLink, jobId, reportInputStream, "$sellerLink-$interval.xlsx")
        reportRepository.updateReportStatus(jobId, ReportStatus.COMPLETED).awaitSingleOrNull()
        reportRepository.setReportId(jobId, reportId).awaitSingleOrNull()
    }

    @Transactional
    suspend fun saveSellerReport(sellerLink: String, interval: Int, jobId: String, report: ByteArray) {
        val reportId: String =
            reportFileService.saveSellerReport(sellerLink, jobId, report.inputStream(), "$sellerLink-$interval.xlsx")
        reportRepository.updateReportStatus(jobId, ReportStatus.COMPLETED).awaitSingleOrNull()
        reportRepository.setReportId(jobId, reportId).awaitSingleOrNull()
    }

    @Transactional
    suspend fun saveCategoryReportV2(
        categoryPublicId: Long,
        categoryPath: List<String>,
        interval: Int,
        jobId: String,
        reportInputStream: InputStream
    ) {
        val reportId: String = reportFileService.saveCategoryReport(
            categoryPublicId,
            categoryPath.joinToString(","),
            jobId,
            reportInputStream,
            "$categoryPublicId-$interval.xlsx"
        )
        reportRepository.updateReportStatus(jobId, ReportStatus.COMPLETED).awaitSingleOrNull()
        reportRepository.setReportId(jobId, reportId).awaitSingleOrNull()
    }

    @Transactional
    suspend fun saveCategoryReport(
        categoryPublicId: Long,
        categoryTitle: String,
        interval: Int,
        jobId: String,
        report: ByteArray
    ) {
        val reportId: String = reportFileService.saveCategoryReport(
            categoryPublicId,
            categoryTitle,
            jobId,
            report.inputStream(),
            "$categoryTitle-$interval.xlsx"
        )
        reportRepository.updateReportStatus(jobId, ReportStatus.COMPLETED).awaitSingleOrNull()
        reportRepository.setReportId(jobId, reportId).awaitSingleOrNull()
    }

    suspend fun getUserShopReportDailyReportCount(userId: String): Long? {
        return reportRepository.countByUserIdAndCreatedAtBetweenAndReportType(
            userId,
            LocalDate.now().atStartOfDay(),
            LocalDate.now().atTime(LocalTime.MAX),
            ReportType.SELLER,
        ).awaitSingleOrNull()
    }

    fun getUserShopReportDailyReportCountV2(userId: String): Mono<Long> {
        return reportRepository.countByUserIdAndCreatedAtBetweenAndReportType(
            userId,
            LocalDate.now().atStartOfDay(),
            LocalDate.now().atTime(LocalTime.MAX),
            ReportType.SELLER,
        )
    }

    suspend fun getUserCategoryReportDailyReportCount(userId: String): Long? {
        return reportRepository.countByUserIdAndCreatedAtBetweenAndReportType(
            userId,
            LocalDate.now().atStartOfDay(),
            LocalDate.now().atTime(LocalTime.MAX),
            ReportType.SELLER,
        ).awaitSingleOrNull()
    }

    fun getUserCategoryReportDailyReportCountV2(userId: String): Mono<Long> {
        return reportRepository.countByUserIdAndCreatedAtBetweenAndReportType(
            userId,
            LocalDate.now().atStartOfDay(),
            LocalDate.now().atTime(LocalTime.MAX),
            ReportType.CATEGORY
        )
    }

}
