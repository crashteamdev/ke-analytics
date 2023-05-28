package dev.crashteam.keanalytics.job

import kotlinx.coroutines.reactor.awaitSingleOrNull
import kotlinx.coroutines.runBlocking
import dev.crashteam.keanalytics.config.properties.KazanExpressProperties
import dev.crashteam.keanalytics.domain.mongo.ReportStatus
import dev.crashteam.keanalytics.extensions.getApplicationContext
import dev.crashteam.keanalytics.report.ReportFileService
import dev.crashteam.keanalytics.repository.mongo.ReportRepository
import org.quartz.Job
import org.quartz.JobExecutionContext
import java.time.LocalDateTime

class ReportCleanUpJob : Job {

    override fun execute(context: JobExecutionContext) {
        val appContext = context.getApplicationContext()
        val reportFileService = appContext.getBean(ReportFileService::class.java)
        val reportRepository = appContext.getBean(ReportRepository::class.java)
        val kazanExpressProperties = appContext.getBean(KazanExpressProperties::class.java)
        runBlocking {
            val minusHours = LocalDateTime.now().minusHours(kazanExpressProperties.reportLiveTimeHours!!.toLong())
            reportFileService.findReportWithTtl(minusHours).forEach {
                val jobId = it.metadata?.get("job_id") as? String
                if (jobId != null) {
                    reportRepository.updateReportStatus(jobId, ReportStatus.DELETED).awaitSingleOrNull()
                }
            }
            reportFileService.deleteReportWithTtl(minusHours)
        }
    }
}
