package dev.crashteam.keanalytics.job

import dev.crashteam.keanalytics.extensions.getApplicationContext
import dev.crashteam.keanalytics.report.ReportFileService
import dev.crashteam.keanalytics.report.ReportService
import dev.crashteam.keanalytics.repository.postgres.ReportRepository
import kotlinx.coroutines.Dispatchers
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
        val categoryPublicId = context.jobDetail.jobDataMap["categoryPublicId"] as? String
            ?: throw IllegalStateException("categoryPublicId can't be null")
        val interval = context.jobDetail.jobDataMap["interval"] as? Int
            ?: throw IllegalStateException("interval can't be null")
        val jobId = context.jobDetail.jobDataMap["job_id"] as? String
            ?: throw IllegalStateException("job_id can't be null")
        val now = LocalDateTime.now().toLocalDate().atStartOfDay()
        val fromTime = now.minusDays(interval.toLong())
        val toTime = now
        runBlocking {
            val tempFilePath = withContext(Dispatchers.IO) {
                Files.createTempFile("link-${UUID.randomUUID()}", "")
            }
            try {
                log.info { "Generating report job. categoryPublicId=$categoryPublicId; jobId=$jobId;" }
                reportFileService.generateReportByCategoryV2(
                    categoryPublicId.toLong(),
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
                    categoryPublicId.toLong(),
                    interval,
                    jobId,
                    tempFilePath.inputStream()
                )
            } catch (e: Exception) {
                log.error(e) { "Failed to generate report. categoryPublicId=$categoryPublicId; interval=$interval; jobId=$jobId" }
                val reportRepository =
                    applicationContext.getBean(ReportRepository::class.java)
                reportRepository.updateReportStatusByJobId(
                    jobId,
                    dev.crashteam.keanalytics.db.model.enums.ReportStatus.failed
                )
            } finally {
                tempFilePath.deleteIfExists()
            }
        }
    }
}

