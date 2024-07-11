package dev.crashteam.keanalytics.stream.listener.aws.analytics

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory
import dev.crashteam.keanalytics.stream.handler.aws.analytics.KeScrapEventHandler
import org.springframework.stereotype.Component

@Component
class KeEventStreamProcessorFactory(
    private val keScrapEventHandlers: List<KeScrapEventHandler>,
) : IRecordProcessorFactory {
    override fun createProcessor(): IRecordProcessor {
        return KeEventStreamListener(keScrapEventHandlers)
    }
}
