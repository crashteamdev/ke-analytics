package dev.crashteam.keanalytics.job

import dev.crashteam.keanalytics.domain.mongo.ReportStatus
import dev.crashteam.keanalytics.domain.mongo.ReportVersion
import dev.crashteam.keanalytics.extensions.getApplicationContext
import dev.crashteam.keanalytics.report.ReportFileService
import dev.crashteam.keanalytics.report.ReportService
import dev.crashteam.keanalytics.repository.mongo.CategoryRepository
import dev.crashteam.keanalytics.repository.mongo.ReportRepository
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.reactor.awaitSingleOrNull
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import mu.KotlinLogging
import org.apache.commons.io.FileUtils
import org.quartz.Job
import org.quartz.JobExecutionContext
import java.math.BigInteger
import java.nio.file.Files
import java.time.LocalDateTime
import java.util.*
import kotlin.io.path.deleteIfExists
import kotlin.io.path.inputStream
import kotlin.io.path.outputStream

private val log = KotlinLogging.logger {}

class GenerateCategoryReportJob : Job {

    override fun execute(context: JobExecutionContext) {
        val applicationContext = context.getApplicationContext()
        val reportFileService = applicationContext.getBean(ReportFileService::class.java)
        val reportService = applicationContext.getBean(ReportService::class.java)
        val categoryPublicId = context.jobDetail.jobDataMap["categoryPublicId"] as? Long
            ?: throw IllegalStateException("categoryPublicId can't be null")
        val categoryPath = context.jobDetail.jobDataMap["categoryPath"] as? String
        val interval = context.jobDetail.jobDataMap["interval"] as? Int
            ?: throw IllegalStateException("interval can't be null")
        val jobId = context.jobDetail.jobDataMap["job_id"] as? String
            ?: throw IllegalStateException("job_id can't be null")
        val userId = context.jobDetail.jobDataMap["user_id"] as? String
        val version = ReportVersion.valueOf(context.jobDetail.jobDataMap["version"] as? String ?: ReportVersion.V1.name)
        val now = LocalDateTime.now().toLocalDate().atStartOfDay()
        val fromTime = now.minusDays(interval.toLong())
        val toTime = now
        runBlocking {
            val tempFilePath = withContext(Dispatchers.IO) {
                Files.createTempFile("link-${UUID.randomUUID()}", "")
            }
            try {
                log.info { "Generating report job. categoryPublicId=$categoryPublicId; categoryPath=$categoryPath; jobId=$jobId; version=$version" }
                if (version == ReportVersion.V2) {
                    reportFileService.generateReportByCategoryV2(
                        categoryPublicId,
                        fromTime,
                        toTime,
                        tempFilePath.outputStream()
                    )
                    log.info(
                        "Save generated category file. name=${tempFilePath.fileName};" +
                                " size=${
                                    FileUtils.byteCountToDisplaySize(
                                        BigInteger.valueOf(
                                            tempFilePath.toFile().length()
                                        )
                                    )
                                }",
                    )
                    reportService.saveCategoryReportV2(
                        categoryPublicId,
                        interval,
                        jobId,
                        tempFilePath.inputStream()
                    )
                } else if (version == ReportVersion.V1) {
                    val categoryRepository = applicationContext.getBean(CategoryRepository::class.java)
                    val categoryDocument = categoryRepository.findByPublicId(categoryPublicId).awaitSingleOrNull()
                        ?: throw IllegalStateException("Unknown category publicId=${categoryPublicId}")
                    val generatedReport =
                        reportFileService.generateReportByCategory(categoryDocument.title, fromTime, toTime, 10000)
                    reportService.saveCategoryReport(
                        categoryPublicId,
                        categoryDocument.title,
                        interval,
                        jobId,
                        generatedReport
                    )
                } else {
                    throw IllegalStateException("Unknown report version: $version")
                }
            } catch (e: Exception) {
                log.error(e) { "Failed to generate report. categoryPublicId=$categoryPublicId; interval=$interval; jobId=$jobId" }
                val reportRepository = applicationContext.getBean(ReportRepository::class.java)
                reportRepository.updateReportStatus(jobId, ReportStatus.FAILED).awaitSingleOrNull()
            } finally {
                tempFilePath.deleteIfExists()
            }
        }
    }
}
