package dev.crashteam.keanalytics.stream.handler.aws.analytics

import com.google.protobuf.Timestamp
import dev.crashteam.ke.scrapper.data.v1.KeProductCategoryPositionChange
import dev.crashteam.ke.scrapper.data.v1.KeScrapperEvent
import dev.crashteam.keanalytics.extensions.toLocalDateTime
import dev.crashteam.keanalytics.repository.clickhouse.CHProductPositionRepository
import dev.crashteam.keanalytics.repository.clickhouse.model.ChProductPosition
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import org.springframework.stereotype.Component

private val log = KotlinLogging.logger {}

@Component
class KeProductPositionEventHandler(
    private val chProductPositionRepository: CHProductPositionRepository
) : KeScrapEventHandler {

    override fun handle(events: List<KeScrapperEvent>) {
        runBlocking {
            val keProductPositionEventWrappers =
                events.map { KeProductPositionEventWrapper(it.eventPayload.keProductPositionChange, it.scrapTime) }
            val saveProductPositionTask = async {
                try {
                    saveProductPosition(keProductPositionEventWrappers)
                } catch (e: Exception) {
                    log.error(e) { "Exception during save product position events" }
                }
            }
            awaitAll(saveProductPositionTask)
        }
    }

    private fun saveProductPosition(keProductPositionEventWrappers: List<KeProductPositionEventWrapper>) {
        val chProductPositions = keProductPositionEventWrappers.map {
            ChProductPosition(
                fetchTime = it.eventTime.toLocalDateTime(),
                productId = it.productCategoryPositionChange.productId,
                skuId = it.productCategoryPositionChange.skuId,
                categoryId = it.productCategoryPositionChange.categoryId,
                position = it.productCategoryPositionChange.position
            )
        }
        chProductPositionRepository.saveProductsPosition(chProductPositions)
        log.info { "Successfully save product position. count=${chProductPositions.size}" }
    }

    override fun isHandle(event: KeScrapperEvent): Boolean {
        return event.eventPayload.hasKeProductPositionChange()
    }

    private data class KeProductPositionEventWrapper(
        val productCategoryPositionChange: KeProductCategoryPositionChange,
        val eventTime: Timestamp,
    )
}
