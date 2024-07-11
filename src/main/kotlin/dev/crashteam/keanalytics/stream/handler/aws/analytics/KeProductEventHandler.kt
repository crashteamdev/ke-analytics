package dev.crashteam.keanalytics.stream.handler.aws.analytics

import dev.crashteam.ke.scrapper.data.v1.KeScrapperEvent
import dev.crashteam.keanalytics.converter.clickhouse.ChKeProductConverterResultWrapper
import dev.crashteam.keanalytics.db.model.tables.pojos.Sellers
import dev.crashteam.keanalytics.repository.clickhouse.CHProductRepository
import dev.crashteam.keanalytics.repository.postgres.SellerRepository
import dev.crashteam.keanalytics.stream.handler.aws.model.KeProductWrapper
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import org.springframework.core.convert.ConversionService
import org.springframework.stereotype.Component

private val log = KotlinLogging.logger {}

@Component
class KeProductEventHandler(
    private val conversionService: ConversionService,
    private val chProductRepository: CHProductRepository,
    private val sellerRepository: SellerRepository,
) : KeScrapEventHandler {

    override fun handle(events: List<KeScrapperEvent>) {
        val keProductChanges = events.map { KeProductWrapper(it.eventPayload.keProductChange, it.scrapTime) }
        log.info { "Consumer product records count ${keProductChanges.size}" }
        runBlocking {
            val saveProductTask = async {
                try {
                    log.info { "Save ${keProductChanges.size} products (NEW)" }
                    val products = keProductChanges.map {
                        conversionService.convert(it, ChKeProductConverterResultWrapper::class.java)!!
                    }.flatMap { it.result }
                    chProductRepository.saveProducts(products)
                } catch (e: Exception) {
                    log.error(e) { "Exception during save products on NEW SCHEMA" }
                }
            }

            val sellerTask = async {
                try {
                    val sellerDetailDocuments = keProductChanges.map {
                        Sellers(
                            it.product.seller.id,
                            it.product.seller.accountId,
                            it.product.seller.sellerTitle,
                            it.product.seller.sellerLink
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

    override fun isHandle(event: KeScrapperEvent): Boolean {
        return event.eventPayload.hasKeProductChange()
    }
}
