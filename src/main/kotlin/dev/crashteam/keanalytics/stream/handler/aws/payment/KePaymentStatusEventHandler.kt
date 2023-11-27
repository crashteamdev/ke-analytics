package dev.crashteam.keanalytics.stream.handler.aws.payment

import dev.crashteam.keanalytics.repository.mongo.PaymentRepository
import dev.crashteam.keanalytics.service.PaymentService
import dev.crashteam.payment.PaymentEvent
import dev.crashteam.payment.PaymentStatus
import kotlinx.coroutines.reactor.awaitSingleOrNull
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import org.springframework.stereotype.Component

private val log = KotlinLogging.logger {}

@Component
class KePaymentStatusEventHandler(
    private val paymentService: PaymentService,
    private val paymentRepository: PaymentRepository,
) : PaymentEventHandler {

    override fun handle(events: List<PaymentEvent>) {
        runBlocking {
            for (event in events) {
                val paymentStatusChanged = event.payload.paymentChange.paymentStatusChanged
                val paymentDocument =
                    paymentRepository.findByPaymentId(paymentStatusChanged.paymentId).awaitSingleOrNull()
                        ?: continue
                val updatePaymentDocument = paymentDocument.copy(status = paymentStatusChanged.status.name)
                when (paymentStatusChanged.status) {
                    PaymentStatus.PAYMENT_STATUS_PENDING,
                    PaymentStatus.PAYMENT_STATUS_CANCELED,
                    PaymentStatus.PAYMENT_STATUS_FAILED -> {
                        paymentRepository.save(updatePaymentDocument).awaitSingleOrNull()
                    }

                    PaymentStatus.PAYMENT_STATUS_SUCCESS -> {
                        paymentService.callbackPayment(
                            paymentId = paymentStatusChanged.paymentId,
                            userId = paymentDocument.userId,
                        )
                    }

                    PaymentStatus.PAYMENT_STATUS_UNKNOWN, PaymentStatus.UNRECOGNIZED -> {
                        log.warn { "Received payment event with unknown status: ${paymentStatusChanged.status}" }
                    }
                }
            }
        }
    }

    override fun isHandle(event: PaymentEvent): Boolean {
        return event.payload.hasPaymentChange() && event.payload.paymentChange.hasPaymentStatusChanged()
    }
}
