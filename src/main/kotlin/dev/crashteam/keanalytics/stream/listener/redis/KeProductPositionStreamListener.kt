package dev.crashteam.keanalytics.stream.listener.redis

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import mu.KotlinLogging
import dev.crashteam.keanalytics.domain.mongo.ProductPositionId
import dev.crashteam.keanalytics.domain.mongo.ProductPositionMetadata
import dev.crashteam.keanalytics.domain.mongo.ProductPositionTSDocument
import dev.crashteam.keanalytics.repository.mongo.ProductPositionRepository
import dev.crashteam.keanalytics.stream.model.KeProductPositionStreamRecord
import org.springframework.data.redis.connection.stream.ObjectRecord
import org.springframework.data.redis.stream.StreamListener
import org.springframework.stereotype.Component
import java.time.Instant

private val log = KotlinLogging.logger {}

@Component
class KeProductPositionStreamListener(
    private val objectMapper: ObjectMapper,
    private val productPositionRepository: ProductPositionRepository,
) : StreamListener<String, ObjectRecord<String, String>> {

    override fun onMessage(message: ObjectRecord<String, String>) {
        try {
            val productPositionStreamRecord = objectMapper.readValue<KeProductPositionStreamRecord>(message.value)
            log.info {
                "Consume product position record from stream." +
                        " productId=${productPositionStreamRecord.productId};" +
                        " skuId=${productPositionStreamRecord.skuId};" +
                        " position=${productPositionStreamRecord.position}"
            }
            val productPositionTSDocument = ProductPositionTSDocument(
                position = productPositionStreamRecord.position,
                metadata = ProductPositionMetadata(
                    id = ProductPositionId(
                        productId = productPositionStreamRecord.productId,
                        skuId = productPositionStreamRecord.skuId
                    ),
                    categoryId = productPositionStreamRecord.categoryId
                ),
                timestamp = Instant.ofEpochMilli(productPositionStreamRecord.time)
            )
            productPositionRepository.save(productPositionTSDocument).doOnSuccess {
                log.info { "Successfully saved product position. " + "" +
                        " productId=${productPositionStreamRecord.productId};" +
                        " skuId=${productPositionStreamRecord.skuId};" +
                        " position=${productPositionStreamRecord.position}" }
            }.subscribe()
        } catch (e: Exception) {
            log.error(e) { "Exception during handle position message. message=${message.value}" }
        }
    }
}
