package dev.crashteam.keanalytics.stream.listener.aws

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker
import org.springframework.scheduling.annotation.Async
import org.springframework.stereotype.Component

@Component
class KeEventStreamAsyncLoop(
    private val paymentStreamWorker: Worker,
) {

    @Async
    fun startPaymentStreamLoop() {
        paymentStreamWorker.run()
    }

}
