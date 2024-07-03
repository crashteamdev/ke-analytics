package dev.crashteam.keanalytics.repository.postgres

import dev.crashteam.keanalytics.db.model.enums.ReportStatus
import dev.crashteam.keanalytics.db.model.enums.ReportType
import dev.crashteam.keanalytics.db.model.tables.Reports.REPORTS
import dev.crashteam.keanalytics.db.model.tables.pojos.Reports
import org.jooq.DSLContext
import org.springframework.stereotype.Repository
import java.io.InputStream
import java.time.LocalDateTime

@Repository
class ReportJooqRepository(
    private val dsl: DSLContext
) : ReportRepository {

    override fun saveNewCategoryReport(report: Reports) {
        val r = REPORTS
        dsl.insertInto(
            r,
            r.REPORT_ID,
            r.JOB_ID,
            r.USER_ID,
            r.CREATED_AT,
            r.INTERVAL,
            r.CATEGORY_ID,
            r.REPORT_TYPE,
            r.STATUS
        ).values(
            report.reportId,
            report.jobId,
            report.userId,
            report.createdAt,
            report.interval,
            report.categoryId,
            report.reportType,
            report.status
        ).onConflictDoNothing().execute()
    }

    override fun saveNewSellerReport(report: Reports) {
        val r = REPORTS
        dsl.insertInto(
            r,
            r.REPORT_ID,
            r.JOB_ID,
            r.USER_ID,
            r.CREATED_AT,
            r.INTERVAL,
            r.SELLER_LINK,
            r.REPORT_TYPE,
            r.STATUS
        ).values(
            report.reportId,
            report.jobId,
            report.userId,
            report.createdAt,
            report.interval,
            report.sellerLink,
            report.reportType,
            report.status
        ).onConflictDoNothing().execute()
    }

    override fun findByJobId(jobId: String): Reports? {
        val r = REPORTS
        return dsl.selectFrom(r)
            .where(r.JOB_ID.eq(jobId))
            .fetchOneInto(Reports::class.java)
    }

    override fun findAllByStatus(reportStatus: ReportStatus): List<Reports> {
        val r = REPORTS
        return dsl.selectFrom(r)
            .where(r.STATUS.eq(reportStatus))
            .fetchInto(Reports::class.java)
    }

    override fun findByRequestIdAndSellerLink(requestId: String, sellerLink: String): Reports? {
        val r = REPORTS
        return dsl.selectFrom(r)
            .where(r.REPORT_ID.eq(requestId).and(r.SELLER_LINK.eq(sellerLink)))
            .fetchOneInto(Reports::class.java)
    }

    override fun findByRequestIdAndCategoryPublicId(requestId: String, categoryPublicId: Long): Reports? {
        val r = REPORTS
        return dsl.selectFrom(r)
            .where(r.REPORT_ID.eq(requestId).and(r.CATEGORY_ID.eq(categoryPublicId.toString())))
            .fetchOneInto(Reports::class.java)
    }

    override fun countByUserIdAndCreatedAtBetweenAndReportType(
        userId: String,
        fromTime: LocalDateTime,
        toTime: LocalDateTime,
        reportType: ReportType
    ): Int {
        val r = REPORTS
        return dsl.selectCount()
            .from(r)
            .where(r.USER_ID.eq(userId).and(r.CREATED_AT.between(fromTime, toTime)).and(r.REPORT_TYPE.eq(reportType)))
            .fetchOne()?.value1() ?: 0
    }

    override fun findByUserIdAndCreatedAtFromTime(userId: String, fromTime: LocalDateTime): List<Reports> {
        val r = REPORTS
        return dsl.selectFrom(r)
            .where(r.USER_ID.eq(userId).and(r.CREATED_AT.greaterOrEqual(fromTime)))
            .fetchInto(Reports::class.java)
    }

    override fun updateReportStatusByJobId(jobId: String, reportStatus: ReportStatus): Int {
        val r = REPORTS
        return dsl.update(r)
            .set(r.STATUS, reportStatus)
            .where(r.JOB_ID.eq(jobId))
            .execute()
    }

    override fun updateReportIdByJobId(jobId: String, reportId: String): Int {
        val r = REPORTS
        return dsl.update(r)
            .set(r.REPORT_ID, reportId)
            .where(r.JOB_ID.eq(jobId))
            .execute()
    }

    override fun saveJobIdFile(jobId: String, inputStream: InputStream): String? {
        val r = REPORTS
        return dsl.update(r)
            .set(r.FILE, inputStream.readAllBytes())
            .where(r.JOB_ID.eq(jobId))
            .returning(r.REPORT_ID)
            .fetchOne()?.getValue(r.REPORT_ID)
    }

    override fun findAllCreatedLessThan(dateTime: LocalDateTime): List<Reports> {
        val r = REPORTS
        return dsl.selectFrom(r)
            .where(r.CREATED_AT.lessOrEqual(dateTime))
            .fetchInto(Reports::class.java)
    }

    override fun removeAllJobFileBeforeDate(dateTime: LocalDateTime): Int {
        val r = REPORTS
        return dsl.update(r)
            .set(r.FILE, null as ByteArray?)
            .where(r.CREATED_AT.lessOrEqual(dateTime))
            .execute()
    }

    override fun getFileByJobId(jobId: String): ByteArray? {
        val r = REPORTS
        return dsl.select(r.FILE)
            .from(r)
            .where(r.JOB_ID.eq(jobId))
            .fetchOne()?.value1()
    }

    override fun getFileByReportId(reportId: String): ByteArray? {
        val r = REPORTS
        return dsl.select(r.FILE)
            .from(r)
            .where(r.REPORT_ID.eq(reportId))
            .fetchOne()?.value1()
    }
}
