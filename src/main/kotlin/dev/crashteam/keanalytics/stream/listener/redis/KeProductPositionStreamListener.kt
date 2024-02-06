package dev.crashteam.keanalytics.stream.listener.redis

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import dev.crashteam.keanalytics.domain.mongo.ProductPositionId
import dev.crashteam.keanalytics.domain.mongo.ProductPositionMetadata
import dev.crashteam.keanalytics.domain.mongo.ProductPositionTSDocument
import dev.crashteam.keanalytics.repository.clickhouse.CHProductPositionRepository
import dev.crashteam.keanalytics.repository.clickhouse.model.ChProductPosition
import dev.crashteam.keanalytics.repository.mongo.ProductPositionRepository
import dev.crashteam.keanalytics.stream.model.KeProductPositionStreamRecord
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import mu.KotlinLogging
import org.springframework.data.redis.connection.stream.ObjectRecord
import org.springframework.stereotype.Component
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId

private val log = KotlinLogging.logger {}

@Component
class KeProductPositionStreamListener(
    private val objectMapper: ObjectMapper,
    private val productPositionRepository: ProductPositionRepository,
    private val chProductPositionRepository: CHProductPositionRepository,
) : BatchStreamListener<String, ObjectRecord<String, String>> {

    override suspend fun onMessage(messages: List<ObjectRecord<String, String>>) {
        val productPositionStreamRecords = messages.map {
            objectMapper.readValue<KeProductPositionStreamRecord>(it.value)
        }
        coroutineScope {
            val oldSaveProductPositionTask = async {
                for (productPositionStreamRecord in productPositionStreamRecords) {
                    oldSaveProductPositionRecord(productPositionStreamRecord)
                }
            }
            val saveProductPositionTask = async {
                saveProductPositionRecords(productPositionStreamRecords)
            }
            awaitAll(oldSaveProductPositionTask, saveProductPositionTask)
        }
    }

    private fun saveProductPositionRecords(productPositionStreamRecords: List<KeProductPositionStreamRecord>) {
        try {
            log.info {
                "Consume product position records from stream. size=${productPositionStreamRecords.size}"
            }
            val chProductPositions = productPositionStreamRecords.map {
                ChProductPosition(
                    fetchTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(it.time), ZoneId.of("UTC")),
                    productId = it.productId,
                    skuId = it.skuId,
                    categoryId = it.categoryId,
                    position = it.position
                )
            }
            chProductPositionRepository.saveProductsPosition(chProductPositions)
        } catch (e: Exception) {
            log.error(e) { "Exception during handle position message. message=${productPositionStreamRecords}" }
        }
    }

    private fun oldSaveProductPositionRecord(productPositionStreamRecord: KeProductPositionStreamRecord) {
        try {
            log.info {
                "[OLD] Consume product position record from stream." +
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
                log.info {
                    "Successfully saved product position. " + "" +
                            " productId=${productPositionStreamRecord.productId};" +
                            " skuId=${productPositionStreamRecord.skuId};" +
                            " position=${productPositionStreamRecord.position}"
                }
            }.subscribe()
        } catch (e: Exception) {
            log.error(e) { "[OLD] Exception during handle position message. message=${productPositionStreamRecord}" }
        }
    }
}
