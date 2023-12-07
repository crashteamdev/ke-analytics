package dev.crashteam.keanalytics.stream.service

import kotlinx.coroutines.reactor.awaitSingleOrNull
import mu.KotlinLogging
import dev.crashteam.keanalytics.stream.listener.redis.BatchStreamListener
import kotlinx.coroutines.reactive.awaitFirstOrNull
import org.springframework.data.domain.Range
import org.springframework.data.redis.connection.ReactiveRedisConnection
import org.springframework.data.redis.connection.stream.ObjectRecord
import org.springframework.data.redis.connection.stream.PendingMessage
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.data.redis.stream.StreamListener
import org.springframework.stereotype.Component
import java.nio.ByteBuffer
import java.time.Duration

private val log = KotlinLogging.logger {}

@Component
class PendingMessageService(
    private val messageReactiveRedisTemplate: ReactiveRedisTemplate<String, String>
) {

    suspend fun receivePendingMessages(
        streamKey: String,
        consumerGroupName: String,
        consumerName: String,
        listener: StreamListener<String, ObjectRecord<String, String>>
    ) {
        messageReactiveRedisTemplate.opsForStream<String, String>().pending(
            streamKey,
            consumerGroupName,
            Range.unbounded<String>(),
            MAX_NUMBER_FETCH
        ).awaitSingleOrNull()?.forEach { pendingMessage ->
            claimMessage(streamKey, consumerGroupName, consumerName, pendingMessage)
            log.info { "Message: ${pendingMessage.idAsString} has been claimed by $consumerGroupName:$consumerName." +
                    " pendingMessage=${pendingMessage}" }
            val messagesToProcess = messageReactiveRedisTemplate.opsForStream<String, String>().range(
                String::class.java,
                streamKey,
                Range.closed(pendingMessage.idAsString, pendingMessage.idAsString)
            ).collectList().awaitSingleOrNull()
            if (messagesToProcess.isNullOrEmpty()) {
                log.warn {
                    "Message is not present." +
                            " It has been either processed or deleted by some other process: ${pendingMessage.idAsString}"
                }
            } else if (pendingMessage.totalDeliveryCount > MAX_RETRY) {
                messageReactiveRedisTemplate.opsForStream<String, String>()
                    .acknowledge(streamKey, consumerGroupName, pendingMessage.idAsString).awaitSingleOrNull()
                log.info { "Message has been added acknowledged case of max_retry attempts: ${pendingMessage.idAsString}" }
            } else {
                for (message in messagesToProcess) {
                    listener.onMessage(message)
                    messageReactiveRedisTemplate.opsForStream<String, String>()
                        .acknowledge(streamKey, consumerGroupName, message.id).awaitSingleOrNull()
                }
            }
        }
    }

    // TODO: fix duplicate code
    suspend fun receiveBatchPendingMessages(
        streamKey: String,
        consumerGroupName: String,
        consumerName: String,
        listener: BatchStreamListener<String, ObjectRecord<String, String>>
    ) {
        messageReactiveRedisTemplate.opsForStream<String, String>().pending(
            streamKey,
            consumerGroupName,
            Range.unbounded<String>(),
            MAX_NUMBER_FETCH
        ).awaitSingleOrNull()?.forEach { pendingMessage ->
            claimMessage(streamKey, consumerGroupName, consumerName, pendingMessage)
            log.info { "Message: ${pendingMessage.idAsString} has been claimed by $consumerGroupName:$consumerName." +
                    " pendingMessage=${pendingMessage}" }
            val messagesToProcess = messageReactiveRedisTemplate.opsForStream<String, String>().range(
                String::class.java,
                streamKey,
                Range.closed(pendingMessage.idAsString, pendingMessage.idAsString)
            ).collectList().awaitSingleOrNull()
            if (messagesToProcess.isNullOrEmpty()) {
                log.warn {
                    "Message is not present." +
                            " It has been either processed or deleted by some other process: ${pendingMessage.idAsString}"
                }
            } else if (pendingMessage.totalDeliveryCount > MAX_RETRY) {
                messageReactiveRedisTemplate.opsForStream<String, String>()
                    .acknowledge(streamKey, consumerGroupName, pendingMessage.idAsString).awaitSingleOrNull()
                log.info { "Message has been added acknowledged case of max_retry attempts: ${pendingMessage.idAsString}" }
            } else {
                listener.onMessage(messagesToProcess)
                val recordIds = messagesToProcess.map { it.id }
                messageReactiveRedisTemplate.opsForStream<String, String>()
                    .acknowledge(streamKey, consumerGroupName, *recordIds.toTypedArray()).awaitSingleOrNull()
            }
        }
    }

    private suspend fun claimMessage(
        streamKey: String,
        consumerGroupName: String,
        consumerName: String,
        pendingMessage: PendingMessage
    ) {
        val redisConnection: ReactiveRedisConnection = messageReactiveRedisTemplate.connectionFactory.reactiveConnection
        redisConnection.streamCommands().xClaim(
            ByteBuffer.wrap(streamKey.toByteArray()),
            consumerGroupName,
            consumerName,
            Duration.ofSeconds(5),
            pendingMessage.id
        ).awaitFirstOrNull()
    }

    private companion object {
        const val MAX_NUMBER_FETCH = 500L
        const val MAX_RETRY = 5
    }
}
