package dev.crashteam.keanalytics.stream.scheduler

import dev.crashteam.keanalytics.config.properties.RedisProperties
import dev.crashteam.keanalytics.extensions.getApplicationContext
import dev.crashteam.keanalytics.stream.listener.redis.BatchStreamListener
import dev.crashteam.keanalytics.stream.listener.redis.KeCategoryStreamListener
import dev.crashteam.keanalytics.stream.listener.redis.KeProductItemStreamListener
import dev.crashteam.keanalytics.stream.listener.redis.KeProductPositionStreamListener
import dev.crashteam.keanalytics.stream.service.PendingMessageService
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import org.quartz.DisallowConcurrentExecution
import org.quartz.Job
import org.quartz.JobExecutionContext
import org.springframework.data.redis.connection.stream.ObjectRecord
import org.springframework.data.redis.stream.StreamListener

private val log = KotlinLogging.logger {}

@DisallowConcurrentExecution
class PendingMessageScheduler : Job {

    override fun execute(context: JobExecutionContext) {
        val appContext = context.getApplicationContext()
        val pendingMessageService = appContext.getBean(PendingMessageService::class.java)
        val redisProperties = appContext.getBean(RedisProperties::class.java)
        val keProductItemStreamListener = appContext.getBean(KeProductItemStreamListener::class.java)
        val keProductPositionStreamListener = appContext.getBean(KeProductPositionStreamListener::class.java)
        val keCategoryStreamListener = appContext.getBean(KeCategoryStreamListener::class.java)
        runBlocking {
            val productPendingMessageTask = async {
                processPendingMessage(
                    streamKey = redisProperties.stream.keProductInfo.streamName,
                    consumerGroup = redisProperties.stream.keProductInfo.consumerGroup,
                    consumerName = redisProperties.stream.keProductInfo.consumerName,
                    batchListener = keProductItemStreamListener,
                    pendingMessageService
                )
            }
            val productPositionPendingMessageTask = async {
                processPendingMessage(
                    streamKey = redisProperties.stream.keProductPosition.streamName,
                    consumerGroup = redisProperties.stream.keProductPosition.consumerGroup,
                    consumerName = redisProperties.stream.keProductPosition.consumerName,
                    listener = keProductPositionStreamListener,
                    pendingMessageService
                )
            }
            val categoryPendingMessageTask = async {
                processPendingMessage(
                    streamKey = redisProperties.stream.keCategoryInfo.streamName,
                    consumerGroup = redisProperties.stream.keCategoryInfo.consumerGroup,
                    consumerName = redisProperties.stream.keCategoryInfo.consumerName,
                    listener = keCategoryStreamListener,
                    pendingMessageService
                )
            }
            val paymentTask = async {
                processPendingMessage(
                    streamKey = redisProperties.stream.payment.streamName,
                    consumerGroup = redisProperties.stream.payment.consumerGroup,
                    consumerName = redisProperties.stream.payment.consumerName,
                    listener = keCategoryStreamListener,
                    pendingMessageService
                )
            }
            awaitAll(
                productPendingMessageTask,
                productPositionPendingMessageTask,
                categoryPendingMessageTask,
                paymentTask
            )
        }
    }

    private suspend fun processPendingMessage(
        streamKey: String,
        consumerGroup: String,
        consumerName: String,
        batchListener: BatchStreamListener<String, ObjectRecord<String, String>>,
        pendingMessageService: PendingMessageService
    ) {
        try {
            log.info { "Processing pending message by consumer $consumerName" }
            pendingMessageService.receiveBatchPendingMessages(
                streamKey = streamKey,
                consumerGroupName = consumerGroup,
                consumerName = consumerName,
                listener = batchListener
            )
        } catch (e: Exception) {
            log.error(e) { "Processing pending message failed" }
        }
    }

    private suspend fun processPendingMessage(
        streamKey: String,
        consumerGroup: String,
        consumerName: String,
        listener: StreamListener<String, ObjectRecord<String, String>>,
        pendingMessageService: PendingMessageService
    ) {
        try {
            log.info { "Processing pending message by consumer $consumerName" }
            pendingMessageService.receivePendingMessages(
                streamKey = streamKey,
                consumerGroupName = consumerGroup,
                consumerName = consumerName,
                listener = listener,
            )
        } catch (e: Exception) {
            log.error(e) { "Processing pending message failed" }
        }
    }
}
