package dev.crashteam.keanalytics.job

import kotlinx.coroutines.reactor.awaitSingleOrNull
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import dev.crashteam.keanalytics.extensions.getApplicationContext
import dev.crashteam.keanalytics.repository.mongo.PaymentRepository
import org.quartz.*
import org.springframework.scheduling.quartz.SimpleTriggerFactoryBean
import java.util.*

private val log = KotlinLogging.logger {}

@DisallowConcurrentExecution
class PaymentMasterJob : Job {

    override fun execute(context: JobExecutionContext) {
        val applicationContext = context.getApplicationContext()
        val paymentRepository = applicationContext.getBean(PaymentRepository::class.java)
        val payments = paymentRepository.findByStatus("pending")
        runBlocking {
            val paymentList = payments.collectList().awaitSingleOrNull() ?: emptyList()
            for (paymentDocument in paymentList) {
                val jobIdentity = "${paymentDocument.paymentId}-payment-Job"
                val jobKey = JobKey(jobIdentity)
                val jobDetail =
                    JobBuilder.newJob(PaymentJob::class.java).withIdentity(jobKey).build()
                val triggerFactoryBean = SimpleTriggerFactoryBean().apply {
                    setName(jobIdentity)
                    setStartTime(Date())
                    setRepeatInterval(30L)
                    setRepeatCount(0)
                    setMisfireInstruction(SimpleTrigger.MISFIRE_INSTRUCTION_FIRE_NOW)
                    setPriority(Int.MAX_VALUE)
                    afterPropertiesSet()
                }.getObject()
                jobDetail.jobDataMap["paymentId"] = paymentDocument.paymentId
                jobDetail.jobDataMap["userId"] = paymentDocument.userId
                jobDetail.jobDataMap["subscriptionType"] = paymentDocument.subscriptionType

                val schedulerFactoryBean = applicationContext.getBean(Scheduler::class.java)
                val checkExists = schedulerFactoryBean.checkExists(jobKey)

                if (checkExists) continue

                try {
                    schedulerFactoryBean.scheduleJob(jobDetail, triggerFactoryBean)
                } catch (e: ObjectAlreadyExistsException) {
                    log.warn { "Task still in progress: $jobIdentity" }
                } catch (e: Exception) {
                    log.error(e) { "Failed to start scheduler job" }
                }
            }
        }
    }
}
