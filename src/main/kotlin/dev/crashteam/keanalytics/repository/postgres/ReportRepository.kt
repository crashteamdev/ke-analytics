package dev.crashteam.keanalytics.repository.postgres

import dev.crashteam.keanalytics.db.model.enums.ReportStatus
import dev.crashteam.keanalytics.db.model.enums.ReportType
import dev.crashteam.keanalytics.db.model.tables.pojos.Reports
import java.io.InputStream
import java.time.LocalDateTime

interface ReportRepository {

    fun saveNewCategoryReport(report: Reports)

    fun saveNewSellerReport(report: Reports)

    fun findByJobId(jobId: String): Reports?

    fun findAllByStatus(reportStatus: ReportStatus): List<Reports>

    fun findByRequestIdAndSellerLink(requestId: String, sellerLink: String): Reports?

    fun findByRequestIdAndCategoryPublicId(requestId: String, categoryPublicId: Long): Reports?

    fun countByUserIdAndCreatedAtBetweenAndReportType(
        userId: String,
        fromTime: LocalDateTime,
        toTime: LocalDateTime,
        reportType: ReportType
    ): Int

    fun findByUserIdAndCreatedAtFromTime(userId: String, fromTime: LocalDateTime): List<Reports>

    fun updateReportStatusByJobId(jobId: String, reportStatus: ReportStatus): Int

    fun updateReportIdByJobId(jobId: String, reportId: String): Int

    fun saveJobIdFile(jobId: String, inputStream: InputStream): String?

    fun findAllCreatedLessThan(dateTime: LocalDateTime): List<Reports>

    fun removeAllJobFileBeforeDate(dateTime: LocalDateTime): Int

    fun getFileByJobId(jobId: String): ByteArray?

    fun getFileByReportId(reportId: String): ByteArray?

}
