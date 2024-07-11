package dev.crashteam.keanalytics.stream.listener.aws.analytics

import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput
import dev.crashteam.ke.scrapper.data.v1.KeScrapperEvent
import dev.crashteam.keanalytics.stream.handler.aws.analytics.KeScrapEventHandler
import dev.crashteam.keanalytics.stream.listener.aws.AwsStreamListener
import mu.KotlinLogging

private val log = KotlinLogging.logger {}

class KeEventStreamListener(
    private val keScrapEventHandlers: List<KeScrapEventHandler>
) : AwsStreamListener {

    private var partitionId: String? = null

    override fun initialize(initializationInput: InitializationInput) {
        this.partitionId = initializationInput.shardId
        log.info { "[Ke-Data-Stream] Initialized partition $partitionId for streaming." }
    }

    override fun processRecords(processRecordsInput: ProcessRecordsInput) {
        val records = processRecordsInput.records
        log.debug {
            "Received Ke scrap events. size=${processRecordsInput.records.size}"
        }
        List(records.size) { i: Int ->
            KeScrapperEvent.parseFrom(records[i].data)
        }.groupBy { entry -> keScrapEventHandlers.find { it.isHandle(entry) } }
            .forEach { (handler, entries) ->
                try {
                    handler?.handle(entries)
                } catch (e: Exception) {
                    log.error(e) { "Failed to handle event" }
                }
            }
        try {
            log.info { "Consume Ke events records count: ${records.size}" }
            processRecordsInput.checkpointer.checkpoint()
        } catch (e: Exception) {
            log.error(e) { "Failed to checkpoint consumed records" }
        }
    }

    override fun shutdown(shutdownInput: ShutdownInput) {
        try {
            log.debug { "[Ke-Data-Stream] Shutting down event processor for $partitionId." +
                    "shutdownReason=${shutdownInput.shutdownReason}" }
//            if (shutdownInput.shutdownReason == ShutdownReason.TERMINATE) {
//                shutdownInput.checkpointer.checkpoint()
//            }
        } catch (e: Exception) {
            log.error(e) { "[Ke-Data-Stream] Failed to checkpoint on shutdown" }
        }
    }
}
