package dev.crashteam.keanalytics.stream.listener.redis

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import dev.crashteam.keanalytics.converter.clickhouse.ChKeProductConverterResultWrapper
import dev.crashteam.keanalytics.repository.clickhouse.CHProductRepository
import dev.crashteam.keanalytics.db.model.tables.pojos.Sellers
import dev.crashteam.keanalytics.stream.model.KeProductItemStreamRecord
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import mu.KotlinLogging
import org.springframework.core.convert.ConversionService
import org.springframework.data.redis.connection.stream.ObjectRecord
import org.springframework.stereotype.Component

private val log = KotlinLogging.logger {}

@Component
class KeProductItemStreamListener(
    private val objectMapper: ObjectMapper,
    private val conversionService: ConversionService,
    private val sellerRepository: dev.crashteam.keanalytics.repository.postgres.SellerRepository,
    private val chProductRepository: CHProductRepository,
) : BatchStreamListener<String, ObjectRecord<String, String>> {

    override suspend fun onMessage(messages: List<ObjectRecord<String, String>>) {
        val keProductItemStreamRecords = messages.map {
            objectMapper.readValue<KeProductItemStreamRecord>(it.value)
        }
        log.info { "Consumer product records count ${keProductItemStreamRecords.size}" }
        coroutineScope {
            // NEW schema (Clickhouse)
            val saveProductTask = async {
                try {
                    log.info { "Save ${keProductItemStreamRecords.size} products (NEW)" }
                    val products = keProductItemStreamRecords.map {
                        conversionService.convert(it, ChKeProductConverterResultWrapper::class.java)!!
                    }.flatMap { it.result }
                    chProductRepository.saveProducts(products)
                } catch (e: Exception) {
                    log.error(e) { "Exception during save products on NEW SCHEMA" }
                }
            }

            val sellerTask = async {
                try {
                    val sellerDetailDocuments = keProductItemStreamRecords.map {
                        Sellers(
                            it.seller.id,
                            it.seller.accountId,
                            it.seller.sellerTitle,
                            it.seller.sellerLink
                        )
                    }.toSet()
                    sellerRepository.saveBatch(sellerDetailDocuments)
                } catch (e: Exception) {
                    log.error(e) { "Exception during save seller info" }
                }
            }
            awaitAll(saveProductTask, sellerTask)
        }
    }
}
