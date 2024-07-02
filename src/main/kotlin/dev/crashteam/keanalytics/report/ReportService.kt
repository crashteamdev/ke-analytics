package dev.crashteam.keanalytics.report

import dev.crashteam.keanalytics.db.model.enums.ReportStatus
import dev.crashteam.keanalytics.db.model.enums.ReportType
import dev.crashteam.keanalytics.repository.postgres.ReportRepository
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.io.InputStream
import java.time.LocalDate
import java.time.LocalTime


@Service
class ReportService(
    private val reportFileService: ReportFileService,
    private val reportRepository: ReportRepository,
) {

    @Transactional
    suspend fun saveSellerReportV2(sellerLink: String, interval: Int, jobId: String, reportInputStream: InputStream) {
        val reportId: String = reportFileService.saveReport(jobId, reportInputStream)
        reportRepository.updateReportStatusByJobId(jobId, ReportStatus.completed)
        reportRepository.updateReportIdByJobId(jobId, reportId)
    }

    @Transactional
    suspend fun saveCategoryReportV2(
        categoryPublicId: Long,
        interval: Int,
        jobId: String,
        reportInputStream: InputStream
    ) {
        val reportId: String = reportFileService.saveReport(jobId, reportInputStream)
        reportRepository.updateReportStatusByJobId(jobId, ReportStatus.completed)
        reportRepository.updateReportIdByJobId(jobId, reportId)
    }

    fun getUserShopReportDailyReportCountV2(userId: String): Int {
        return reportRepository.countByUserIdAndCreatedAtBetweenAndReportType(
            userId,
            LocalDate.now().atStartOfDay(),
            LocalDate.now().atTime(LocalTime.MAX),
            ReportType.seller
        )
    }

    fun getUserCategoryReportDailyReportCountV2(userId: String): Int {
        return reportRepository.countByUserIdAndCreatedAtBetweenAndReportType(
            userId,
            LocalDate.now().atStartOfDay(),
            LocalDate.now().atTime(LocalTime.MAX),
            ReportType.category
        )
    }
}
