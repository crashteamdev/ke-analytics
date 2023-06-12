package dev.crashteam.keanalytics.stream.scheduler

import dev.crashteam.keanalytics.config.properties.RedisProperties
import dev.crashteam.keanalytics.stream.listener.BatchStreamListener
import dev.crashteam.keanalytics.stream.listener.KeCategoryStreamListener
import dev.crashteam.keanalytics.stream.listener.KeProductItemStreamListener
import dev.crashteam.keanalytics.stream.listener.KeProductPositionStreamListener
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.reactor.awaitSingleOrNull
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import org.springframework.data.redis.connection.stream.Consumer
import org.springframework.data.redis.connection.stream.ObjectRecord
import org.springframework.data.redis.connection.stream.ReadOffset
import org.springframework.data.redis.connection.stream.StreamOffset
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.data.redis.stream.StreamListener
import org.springframework.data.redis.stream.StreamReceiver
import org.springframework.retry.support.RetryTemplate
import org.springframework.stereotype.Component
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import javax.annotation.PostConstruct

private val log = KotlinLogging.logger {}

@Component
class MessageScheduler(
    private val keProductPositionSubscription: StreamReceiver<String, ObjectRecord<String, String>>,
    private val keProductSubscription: StreamReceiver<String, ObjectRecord<String, String>>,
    private val keCategorySubscription: StreamReceiver<String, ObjectRecord<String, String>>,
    private val keProductPositionStreamListener: KeProductPositionStreamListener,
    private val keProductStreamListener: KeProductItemStreamListener,
    private val keCategoryStreamListener: KeCategoryStreamListener,
    private val redisProperties: RedisProperties,
    private val messageReactiveRedisTemplate: ReactiveRedisTemplate<String, String>,
    private val retryTemplate: RetryTemplate,
) {

    val executor: ExecutorService = Executors.newSingleThreadExecutor()

    @PostConstruct
    fun receiveMessages() {
        executor.submit {
            retryTemplate.execute<Unit, Exception> {
                runBlocking {
                    log.info { "Start receiving stream messages" }
                    try {
                        val createPositionConsumerTask = launch {
                            createConsumer(
                                redisProperties.stream.keProductPosition.streamName,
                                redisProperties.stream.keProductPosition.consumerGroup,
                                redisProperties.stream.keProductPosition.consumerName,
                                keProductPositionSubscription,
                                keProductPositionStreamListener
                            )
                        }
                        val createProductConsumerTask = launch {
                            creatBatchConsumer(
                                redisProperties.stream.keProductInfo.streamName,
                                redisProperties.stream.keProductInfo.consumerGroup,
                                redisProperties.stream.keProductInfo.consumerName,
                                keProductSubscription,
                                keProductStreamListener
                            )
                        }
                        val createCategoryConsumerTask = launch {
                            createConsumer(
                                redisProperties.stream.keCategoryInfo.streamName,
                                redisProperties.stream.keCategoryInfo.consumerGroup,
                                redisProperties.stream.keCategoryInfo.consumerName,
                                keCategorySubscription,
                                keCategoryStreamListener
                            )
                        }
                        listOf(
                            createPositionConsumerTask,
                            createProductConsumerTask,
                            createCategoryConsumerTask
                        ).joinAll()
                    } catch (e: Exception) {
                        log.error(e) { "Exception during creating consumers" }
                        throw e
                    }
                    log.info { "End of receiving stream messages" }
                }
            }
        }
    }

    private suspend fun createConsumer(
        streamKey: String,
        consumerGroup: String,
        consumerName: String,
        receiver: StreamReceiver<String, ObjectRecord<String, String>>,
        listener: StreamListener<String, ObjectRecord<String, String>>,
    ) {
        val consumer = Consumer.from(consumerGroup, consumerName)
        val objectRecords = receiver.receive(
            consumer,
            StreamOffset.create(streamKey, ReadOffset.lastConsumed())
        ).collectList().awaitSingleOrNull()
        objectRecords?.forEach {
            listener.onMessage(it)
            messageReactiveRedisTemplate.opsForStream<String, String>().acknowledge(streamKey, consumerGroup, it.id)
                .awaitSingleOrNull()
        }
    }

    private suspend fun creatBatchConsumer(
        streamKey: String,
        consumerGroup: String,
        consumerName: String,
        receiver: StreamReceiver<String, ObjectRecord<String, String>>,
        listener: BatchStreamListener<String, ObjectRecord<String, String>>,
    ) {
        val consumer = Consumer.from(consumerGroup, consumerName)
        val objectRecords = receiver.receive(
            consumer,
            StreamOffset.create(streamKey, ReadOffset.lastConsumed())
        ).collectList().awaitSingleOrNull()
        if (objectRecords != null) {
            listener.onMessage(objectRecords)
            val recordIds = objectRecords.map { it.id }
            log.info { "Acknowledge. streamKey=$streamKey; consumerGroup=$consumerGroup; recordIds=$recordIds " }
            messageReactiveRedisTemplate.opsForStream<String, String>()
                .acknowledge(streamKey, consumerGroup, *recordIds.toTypedArray()).awaitSingleOrNull()
        }
//        receiver.receive(
//            consumer,
//            StreamOffset.create(streamKey, ReadOffset.lastConsumed())
//        ).bufferTimeout(
//            redisProperties.stream.maxBatchSize,
//            java.time.Duration.ofMillis(redisProperties.stream.batchBufferDurationMs)
//        ).onBackpressureBuffer()
//            .parallel(redisProperties.stream.batchParallelCount)
//            .runOn(Schedulers.newParallel("redis-stream-batch", redisProperties.stream.batchParallelCount))
//            .flatMap { records ->
//                listener.onMessage(records)
//                records.map { it.id }.toMono()
//            }.flatMap { recordIds ->
//                log.info { "Acknowledge. streamKey=$streamKey; consumerGroup=$consumerGroup; recordIds=$recordIds " }
//                messageReactiveRedisTemplate.opsForStream<String, String>()
//                    .acknowledge(streamKey, consumerGroup, *recordIds.toTypedArray())
//            }.doOnError {
//                log.warn(it) { "Error during consumer task" }
//            }.subscribe()
    }

    private companion object {
        const val MAX_RETRY_ATTEMPTS = 30L
        const val RETRY_DURATION_SEC = 60L
    }
}
