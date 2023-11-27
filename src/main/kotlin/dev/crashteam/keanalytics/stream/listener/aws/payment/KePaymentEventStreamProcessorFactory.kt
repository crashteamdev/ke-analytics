package dev.crashteam.keanalytics.stream.listener.aws.payment

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory
import dev.crashteam.keanalytics.stream.listener.aws.payment.KePaymentEventStreamListener
import dev.crashteam.keanalytics.stream.handler.aws.payment.PaymentEventHandler
import org.springframework.stereotype.Component

@Component
class KePaymentEventStreamProcessorFactory(
    private val paymentEventHandler: List<PaymentEventHandler>
) : IRecordProcessorFactory {
    override fun createProcessor(): IRecordProcessor {
        return KePaymentEventStreamListener(paymentEventHandler)
    }
}
