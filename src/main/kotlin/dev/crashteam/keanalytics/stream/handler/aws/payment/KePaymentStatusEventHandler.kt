package dev.crashteam.keanalytics.stream.handler.aws.payment

import dev.crashteam.keanalytics.repository.postgres.PaymentRepository
import dev.crashteam.keanalytics.service.PaymentService
import dev.crashteam.payment.PaymentEvent
import dev.crashteam.payment.PaymentStatus
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
                when (paymentStatusChanged.status) {
                    PaymentStatus.PAYMENT_STATUS_PENDING,
                    PaymentStatus.PAYMENT_STATUS_CANCELED,
                    PaymentStatus.PAYMENT_STATUS_FAILED -> {
                        log.warn { "Failed payment. paymentId=${paymentStatusChanged.paymentId}" }
                        val paymentStatus = mapPaymentStatus(paymentStatusChanged.status)
                        paymentRepository.updatePaymentStatus(paymentStatusChanged.paymentId, paymentStatus, false)
                    }

                    PaymentStatus.PAYMENT_STATUS_SUCCESS -> {
                        log.info { "Success payment. paymentId=${paymentStatusChanged.paymentId}" }
                        val paymentEntity = paymentRepository.findByPaymentId(paymentStatusChanged.paymentId)
                        if (paymentEntity?.status != "success") {
                            paymentService.callbackPayment(
                                paymentId = paymentStatusChanged.paymentId,
                                userId = paymentEntity?.userId!!,
                            )
                        }
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

    private fun mapPaymentStatus(paymentStatus: PaymentStatus): String {
        return when (paymentStatus) {
            PaymentStatus.PAYMENT_STATUS_PENDING -> "pending"
            PaymentStatus.PAYMENT_STATUS_SUCCESS -> "success"
            PaymentStatus.PAYMENT_STATUS_CANCELED -> "canceled"
            PaymentStatus.PAYMENT_STATUS_FAILED -> "failed"
            PaymentStatus.PAYMENT_STATUS_UNKNOWN, PaymentStatus.UNRECOGNIZED -> "unknown"
        }
    }
}
