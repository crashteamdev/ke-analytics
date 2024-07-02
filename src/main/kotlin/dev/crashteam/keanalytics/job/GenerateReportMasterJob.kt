package dev.crashteam.keanalytics.job

import dev.crashteam.keanalytics.db.model.tables.pojos.Reports
import dev.crashteam.keanalytics.extensions.getApplicationContext
import dev.crashteam.keanalytics.repository.postgres.ReportRepository
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import org.quartz.*
import org.springframework.scheduling.quartz.SimpleTriggerFactoryBean
import java.util.*

private val log = KotlinLogging.logger {}

class GenerateReportMasterJob : Job {

    override fun execute(context: JobExecutionContext) {
        val applicationContext = context.getApplicationContext()
        val reportRepository = applicationContext.getBean(ReportRepository::class.java)
        runBlocking {
            val reportDocuments =
                reportRepository.findAllByStatus(dev.crashteam.keanalytics.db.model.enums.ReportStatus.processing)
            val schedulerFactoryBean = applicationContext.getBean(Scheduler::class.java)
            reportDocuments.forEach { reportDoc ->
                when (reportDoc.reportType) {
                    dev.crashteam.keanalytics.db.model.enums.ReportType.seller -> {
                        log.info { "Schedule job report. sellerLink=${reportDoc.sellerLink}; jobId=${reportDoc.jobId}" }
                        val jobIdentity = "${reportDoc.sellerLink}-seller-report-${reportDoc.jobId}-Job"
                        scheduleShopReportJob(jobIdentity, reportDoc, schedulerFactoryBean)
                    }

                    dev.crashteam.keanalytics.db.model.enums.ReportType.category -> {
                        log.info { "Schedule job report. categoryId=${reportDoc.categoryId}; jobId=${reportDoc.jobId}" }
                        val jobIdentity = "${reportDoc.categoryId}-category-report-${reportDoc.jobId}-Job"
                        schedulerCategoryReportJob(jobIdentity, reportDoc, schedulerFactoryBean)
                    }

                    else -> {
                        if (reportDoc.sellerLink != null) {
                            // Execute shop report
                            val jobIdentity = "${reportDoc.sellerLink}-seller-report-${reportDoc.jobId}-Job"
                            scheduleShopReportJob(jobIdentity, reportDoc, schedulerFactoryBean)
                        }
                    }
                }
            }
        }
    }

    private fun scheduleShopReportJob(jobIdentity: String, reportDoc: Reports, schedulerFactoryBean: Scheduler) {
        val jobKey = JobKey(jobIdentity)
        val jobDetail =
            JobBuilder.newJob(GenerateSellerReportJob::class.java).withIdentity(jobKey)
                .usingJobData(JobDataMap(
                    mapOf(
                        "sellerLink" to reportDoc.sellerLink,
                        "interval" to reportDoc.interval,
                        "job_id" to reportDoc.jobId,
                        "user_id" to reportDoc.userId
                    )
                )).build()
        val triggerFactoryBean = SimpleTriggerFactoryBean().apply {
            setName(jobIdentity)
            setStartTime(Date())
            setRepeatInterval(30L)
            setRepeatCount(0)
            setMisfireInstruction(SimpleTrigger.MISFIRE_INSTRUCTION_FIRE_NOW)
            setPriority(Int.MAX_VALUE)
            afterPropertiesSet()
        }.getObject()
        if (!schedulerFactoryBean.checkExists(jobKey)) {
            schedulerFactoryBean.scheduleJob(jobDetail, triggerFactoryBean)
        }
    }

    private fun schedulerCategoryReportJob(
        jobIdentity: String,
        reportDoc: Reports,
        schedulerFactoryBean: Scheduler
    ) {
        val jobKey = JobKey(jobIdentity)
        val jobDetail =
            JobBuilder.newJob(GenerateCategoryReportJob::class.java).withIdentity(jobKey)
                .usingJobData(
                    JobDataMap(
                        mapOf<String, Any>(
                            "categoryPublicId" to reportDoc.categoryId,
                            "interval" to reportDoc.interval,
                            "job_id" to reportDoc.jobId,
                            "user_id" to reportDoc.userId
                        )
                    )

                ).build()
        val triggerFactoryBean = SimpleTriggerFactoryBean().apply {
            setName(jobIdentity)
            setStartTime(Date())
            setRepeatInterval(30L)
            setRepeatCount(0)
            setMisfireInstruction(SimpleTrigger.MISFIRE_INSTRUCTION_FIRE_NOW)
            setPriority(Int.MAX_VALUE)
            afterPropertiesSet()
        }.getObject()
        if (!schedulerFactoryBean.checkExists(jobKey)) {
            schedulerFactoryBean.scheduleJob(jobDetail, triggerFactoryBean)
        }
    }
}
