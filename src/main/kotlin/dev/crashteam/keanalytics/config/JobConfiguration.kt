package dev.crashteam.keanalytics.config

import dev.crashteam.keanalytics.config.properties.KazanExpressProperties
import dev.crashteam.keanalytics.job.GenerateReportMasterJob
import dev.crashteam.keanalytics.job.PaymentMasterJob
import dev.crashteam.keanalytics.job.ReportCleanUpJob
import dev.crashteam.keanalytics.job.*
import dev.crashteam.keanalytics.stream.scheduler.PendingMessageScheduler
import org.quartz.*
import org.quartz.impl.JobDetailImpl
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Configuration
import javax.annotation.PostConstruct

@Configuration
@ConditionalOnProperty(
    value = ["kazanex.scheduleEnabled"],
    havingValue = "true",
    matchIfMissing = true
)
class JobConfiguration(
    private val kazanExpressProperties: KazanExpressProperties,
) {

    @Autowired
    private lateinit var schedulerFactoryBean: Scheduler

    @PostConstruct
    fun init() {
//        schedulerFactoryBean.addJob(paymentJob(), true, true)
//        if (!schedulerFactoryBean.checkExists(TriggerKey(PAYMENT_JOB, PAYMENT_JOB_GROUP))) {
//            schedulerFactoryBean.scheduleJob(triggerPaymentJob())
//        }
        schedulerFactoryBean.addJob(reportCleanupJob(), true, true)
        if (!schedulerFactoryBean.checkExists(TriggerKey(REPORT_CLEANUP_JOB, REPORT_CLEANUP_GROUP))) {
            schedulerFactoryBean.scheduleJob(triggerReportCleanupJob())
        }
        schedulerFactoryBean.addJob(reportGenerateMasterJob(), true, true)
        if (!schedulerFactoryBean.checkExists(TriggerKey(REPORT_GENERATE_MASTER_JOB, REPORT_GENERATE_MASTER_GROUP))) {
            schedulerFactoryBean.scheduleJob(triggerReportGenerateMasterJob())
        }
        schedulerFactoryBean.addJob(pendingMessageJob(), true, true)
        if (!schedulerFactoryBean.checkExists(TriggerKey(PENDING_MESSAGE_JOB, PENDING_MESSAGE_GROUP))) {
            schedulerFactoryBean.scheduleJob(triggerPendingMessageJob())
        }
    }

    private fun paymentJob(): JobDetailImpl {
        val jobDetail = JobDetailImpl()
        jobDetail.key = JobKey(PAYMENT_JOB, PAYMENT_JOB_GROUP)
        jobDetail.jobClass = PaymentMasterJob::class.java

        return jobDetail
    }

    private fun triggerPaymentJob(): CronTrigger {
        return TriggerBuilder.newTrigger()
            .forJob(paymentJob())
            .withIdentity(PAYMENT_JOB, PAYMENT_JOB_GROUP)
            .withSchedule(CronScheduleBuilder.cronSchedule(kazanExpressProperties.paymentCron))
            .withPriority(Int.MAX_VALUE)
            .build()
    }

    private fun reportCleanupJob(): JobDetailImpl {
        val jobDetail = JobDetailImpl()
        jobDetail.key = JobKey(REPORT_CLEANUP_JOB, REPORT_CLEANUP_GROUP)
        jobDetail.jobClass = ReportCleanUpJob::class.java

        return jobDetail
    }

    private fun triggerReportCleanupJob(): CronTrigger {
        return TriggerBuilder.newTrigger()
            .forJob(reportCleanupJob())
            .withIdentity(REPORT_CLEANUP_JOB, REPORT_CLEANUP_GROUP)
            .withSchedule(CronScheduleBuilder.cronSchedule(kazanExpressProperties.reportCleanUpCron))
            .build()
    }

    private fun reportGenerateMasterJob(): JobDetailImpl {
        val jobDetail = JobDetailImpl()
        jobDetail.key = JobKey(REPORT_GENERATE_MASTER_JOB, REPORT_GENERATE_MASTER_GROUP)
        jobDetail.jobClass = GenerateReportMasterJob::class.java

        return jobDetail
    }

    private fun triggerReportGenerateMasterJob(): CronTrigger {
        return TriggerBuilder.newTrigger()
            .forJob(reportGenerateMasterJob())
            .withIdentity(REPORT_GENERATE_MASTER_JOB, REPORT_GENERATE_MASTER_GROUP)
            .withSchedule(CronScheduleBuilder.cronSchedule(kazanExpressProperties.reportGenerateCron))
            .withPriority(Int.MAX_VALUE)
            .build()
    }

    private fun pendingMessageJob(): JobDetailImpl {
        val jobDetail = JobDetailImpl()
        jobDetail.key = JobKey(PENDING_MESSAGE_JOB, PENDING_MESSAGE_GROUP)
        jobDetail.jobClass = PendingMessageScheduler::class.java

        return jobDetail
    }

    private fun triggerPendingMessageJob(): CronTrigger {
        return TriggerBuilder.newTrigger()
            .forJob(pendingMessageJob())
            .withIdentity(PENDING_MESSAGE_JOB, PENDING_MESSAGE_GROUP)
            .withSchedule(CronScheduleBuilder.cronSchedule(kazanExpressProperties.pendingMessageCron))
            .withPriority(Int.MAX_VALUE)
            .build()
    }

    companion object {
        const val PAYMENT_JOB = "paymentJob"
        const val PAYMENT_JOB_GROUP = "paymentJobGroup"
        const val CATEGORY_COLLECTOR_JOB = "categoryCollectorJob"
        const val CATEGORY_COLLECTOR_GROUP = "categoryCollectorJobGroup"
        const val CATEGORY_PRODUCT_MASTER_JOB = "categoryProductMasterJob"
        const val CATEGORY_PRODUCT_MASTER_GROUP = "categoryProductMasterJobGroup"
        const val CATEGORY_PRODUCT_MASTER_JOB_V2 = "categoryProductMasterJobV2"
        const val CATEGORY_PRODUCT_MASTER_GROUP_V2 = "categoryProductMasterJobGroupV2"
        const val SELLER_COLLECTOR_MASTER_JOB = "sellerCollectorMasterJobV2"
        const val SELLER_COLLECTOR_MASTER_GROUP = "sellerCollectorMasterJobGroupV2"
        const val REPORT_CLEANUP_JOB = "reportCleanupJob"
        const val REPORT_CLEANUP_GROUP = "reportCleanupGroup"
        const val REPORT_GENERATE_MASTER_JOB = "reportGenerateMasterJob"
        const val REPORT_GENERATE_MASTER_GROUP = "reportGenerateMasterGroup"
        const val PRODUCT_POSITION_MASTER_JOB = "productPositionMasterJob"
        const val PRODUCT_POSITION_MASTER_GROUP = "productPositionMasterGroup"
        const val PENDING_MESSAGE_JOB = "pendingMessageJob"
        const val PENDING_MESSAGE_GROUP = "pendingMessageGroup"
    }
}
