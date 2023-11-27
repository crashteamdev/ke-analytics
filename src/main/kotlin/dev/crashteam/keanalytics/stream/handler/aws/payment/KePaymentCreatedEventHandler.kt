package dev.crashteam.keanalytics.stream.handler.aws.payment

import dev.crashteam.keanalytics.domain.mongo.PaymentDocument
import dev.crashteam.keanalytics.extensions.toLocalDateTime
import dev.crashteam.keanalytics.repository.mongo.PaymentRepository
import dev.crashteam.payment.KeAnalyticsContext
import dev.crashteam.payment.PaymentEvent
import kotlinx.coroutines.reactor.awaitSingleOrNull
import kotlinx.coroutines.runBlocking
import org.springframework.stereotype.Component

@Component
class KePaymentCreatedEventHandler(
    private val paymentRepository: PaymentRepository,
) : PaymentEventHandler {

    override fun handle(events: List<PaymentEvent>) {
        runBlocking {
            for (paymentEvent in events) {
                val paymentCreated = paymentEvent.payload.paymentChange.paymentCreated
                val paymentDocument = paymentRepository.findByPaymentId(paymentCreated.paymentId).awaitSingleOrNull()

                if (paymentDocument != null) continue

                val newPaymentDocument = PaymentDocument(
                    paymentId = paymentCreated.paymentId,
                    userId = paymentCreated.userId,
                    createdAt = paymentCreated.createdAt.toLocalDateTime(),
                    status = paymentCreated.status.name,
                    paid = false,
                    amount = paymentCreated.amount.value.toBigDecimal().movePointLeft(2),
                    multiply = paymentCreated.userPaidService.paidService.context.multiply.toShort(),
                    subscriptionType = mapProtoSubscriptionPlan(
                        paymentCreated.userPaidService.paidService.context.keAnalyticsContext.plan
                    )
                )
                paymentRepository.save(newPaymentDocument).awaitSingleOrNull()
            }
        }
    }

    override fun isHandle(event: PaymentEvent): Boolean {
        return event.payload.hasPaymentChange() &&
                event.payload.paymentChange.hasPaymentCreated() &&
                event.payload.paymentChange.paymentCreated.hasUserPaidService() &&
                event.payload.paymentChange.paymentCreated.userPaidService.paidService.context.hasKeAnalyticsContext()
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
