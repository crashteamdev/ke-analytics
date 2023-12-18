package dev.crashteam.keanalytics.stream.handler.aws.payment

import dev.crashteam.keanalytics.domain.mongo.PaymentDocument
import dev.crashteam.keanalytics.extensions.toLocalDateTime
import dev.crashteam.keanalytics.repository.mongo.PaymentRepository
import dev.crashteam.keanalytics.service.PaymentService
import dev.crashteam.payment.KeAnalyticsContext
import dev.crashteam.payment.PaymentCreated
import dev.crashteam.payment.PaymentEvent
import dev.crashteam.payment.PaymentStatus
import kotlinx.coroutines.reactor.awaitSingleOrNull
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import org.springframework.core.convert.ConversionService
import org.springframework.stereotype.Component

private val log = KotlinLogging.logger {}

@Component
class KePaymentCreatedEventHandler(
    private val paymentRepository: PaymentRepository,
    private val paymentService: PaymentService,
) : PaymentEventHandler {

    override fun handle(events: List<PaymentEvent>) {
        runBlocking {
            for (paymentEvent in events) {
                val paymentCreated = paymentEvent.payload.paymentChange.paymentCreated
                val paymentDocument = paymentRepository.findByPaymentId(paymentCreated.paymentId).awaitSingleOrNull()

                if (paymentDocument != null) continue

                if (paymentCreated.status == PaymentStatus.PAYMENT_STATUS_SUCCESS) {
                    log.info { "Create payment with final state. paymentId=${paymentCreated.paymentId}; userId=${paymentCreated.userId}" }
                    createPayment(paymentCreated)
                    paymentService.callbackPayment(
                        paymentId = paymentCreated.paymentId,
                        userId = paymentCreated.userId,
                    )
                } else {
                    log.info { "Create payment. paymentId=${paymentCreated.paymentId}; userId=${paymentCreated.userId}" }
                    createPayment(paymentCreated)
                }
            }
        }
    }

    private suspend fun createPayment(paymentCreated: PaymentCreated) {
        val newPaymentDocument = PaymentDocument(
            paymentId = paymentCreated.paymentId,
            userId = paymentCreated.userId,
            createdAt = paymentCreated.createdAt.toLocalDateTime(),
            status = mapPaymentStatus(paymentCreated.status),
            paid = false,
            amount = paymentCreated.amount.value.toBigDecimal().movePointLeft(2),
            multiply = paymentCreated.userPaidService.paidService.context.multiply.toShort(),
            subscriptionType = mapProtoSubscriptionPlan(
                paymentCreated.userPaidService.paidService.context.keAnalyticsContext.plan
            )
        )
        paymentRepository.save(newPaymentDocument).awaitSingleOrNull()
    }

    override fun isHandle(event: PaymentEvent): Boolean {
        return event.payload.hasPaymentChange() &&
                event.payload.paymentChange.hasPaymentCreated() &&
                event.payload.paymentChange.paymentCreated.hasUserPaidService() &&
                event.payload.paymentChange.paymentCreated.userPaidService.paidService.context.hasKeAnalyticsContext()
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

    private fun mapProtoSubscriptionPlan(kePlan: KeAnalyticsContext.KeAnalyticsPlan): Int {
        return when (kePlan.planCase) {
            KeAnalyticsContext.KeAnalyticsPlan.PlanCase.DEFAULT_PLAN -> 1
            KeAnalyticsContext.KeAnalyticsPlan.PlanCase.ADVANCED_PLAN -> 2
            KeAnalyticsContext.KeAnalyticsPlan.PlanCase.PRO_PLAN -> 3
            KeAnalyticsContext.KeAnalyticsPlan.PlanCase.PLAN_NOT_SET -> {
                throw IllegalStateException("Unknown paid service plan: $kePlan")
            }
        }
    }
}
