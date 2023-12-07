package dev.crashteam.keanalytics.stream.listener.aws

import org.springframework.stereotype.Component
import javax.annotation.PostConstruct

@Component
class KeEventStreamInitializer(
    private val keEventStreamAsyncLoop: KeEventStreamAsyncLoop
) {
    @PostConstruct
    fun initialize() {
        //keEventStreamAsyncLoop.startPaymentStreamLoop()
    }
}
