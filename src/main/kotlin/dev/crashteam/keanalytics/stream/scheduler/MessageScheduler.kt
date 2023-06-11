package dev.crashteam.keanalytics.stream.scheduler

import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import dev.crashteam.keanalytics.config.properties.RedisProperties
import dev.crashteam.keanalytics.stream.listener.BatchStreamListener
import dev.crashteam.keanalytics.stream.listener.KeCategoryStreamListener
import dev.crashteam.keanalytics.stream.listener.KeProductItemStreamListener
import dev.crashteam.keanalytics.stream.listener.KeProductPositionStreamListener
import org.springframework.data.redis.connection.stream.Consumer
import org.springframework.data.redis.connection.stream.ObjectRecord
import org.springframework.data.redis.connection.stream.ReadOffset
import org.springframework.data.redis.connection.stream.StreamOffset
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.data.redis.stream.StreamListener
import org.springframework.data.redis.stream.StreamReceiver
import org.springframework.retry.support.RetryTemplate
import org.springframework.stereotype.Component
import reactor.core.scheduler.Schedulers
import reactor.util.retry.Retry
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
                    try {
                        val createPositionConsumerTask = async {
                            createConsumer(
                                redisProperties.stream.keProductPosition.streamName,
                                redisProperties.stream.keProductPosition.consumerGroup,
                                redisProperties.stream.keProductPosition.consumerName,
                                keProductPositionSubscription,
                                keProductPositionStreamListener
                            )
                        }
                        val createProductConsumerTask = async {
                            creatBatchConsumer(
                                redisProperties.stream.keProductInfo.streamName,
                                redisProperties.stream.keProductInfo.consumerGroup,
                                redisProperties.stream.keProductInfo.consumerName,
                                keProductSubscription,
                                keProductStreamListener
                            )
                        }
                        val createCategoryConsumerTask = async {
                            createConsumer(
                                redisProperties.stream.keCategoryInfo.streamName,
                                redisProperties.stream.keCategoryInfo.consumerGroup,
                                redisProperties.stream.keCategoryInfo.consumerName,
                                keCategorySubscription,
                                keCategoryStreamListener
                            )
                        }
                        awaitAll(createPositionConsumerTask, createProductConsumerTask, createCategoryConsumerTask)
                    } catch (e: Exception) {
                        log.error(e) { "Exception during creating consumers" }
                        throw e
                    }
                    log.info { "End of receiving stream messages" }
                }
            }
        }
    }

    private fun createConsumer(
        streamKey: String,
        consumerGroup: String,
        consumerName: String,
        receiver: StreamReceiver<String, ObjectRecord<String, String>>,
        listener: StreamListener<String, ObjectRecord<String, String>>,
    ) {
        val consumer = Consumer.from(consumerGroup, consumerName)
        receiver.receive(
            consumer,
            StreamOffset.create(streamKey, ReadOffset.lastConsumed())
        ).publishOn(Schedulers.boundedElastic()).doOnNext {
            listener.onMessage(it)
            messageReactiveRedisTemplate.opsForStream<String, String>()
                .acknowledge(streamKey, consumerGroup, it.id).subscribe()
        }.retryWhen(
            Retry.fixedDelay(MAX_RETRY_ATTEMPTS, java.time.Duration.ofSeconds(RETRY_DURATION_SEC)).doBeforeRetry {
                log.warn(it.failure()) { "Error during consumer task" }
            }).subscribe()
    }

    private fun creatBatchConsumer(
        streamKey: String,
        consumerGroup: String,
        consumerName: String,
        receiver: StreamReceiver<String, ObjectRecord<String, String>>,
        listener: BatchStreamListener<String, ObjectRecord<String, String>>,
    ) {
        val consumer = Consumer.from(consumerGroup, consumerName)
        receiver.receive(
            consumer,
            StreamOffset.create(streamKey, ReadOffset.lastConsumed())
        ).publishOn(Schedulers.boundedElastic())
            .doOnNext { records ->
                listener.onMessage(listOf(records))
                //val recordIds = records.map { it.id }
                messageReactiveRedisTemplate.opsForStream<String, String>()
                    .acknowledge(streamKey, consumerGroup, records.id).subscribe()
            }.doOnError {
                log.warn(it) { "Error during consumer task" }
            }.subscribe()
    }

    private companion object {
        const val MAX_RETRY_ATTEMPTS = 30L
        const val RETRY_DURATION_SEC = 60L
    }
}
