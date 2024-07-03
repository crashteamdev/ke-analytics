package dev.crashteam.keanalytics.repository.postgres

import dev.crashteam.keanalytics.db.model.tables.Payment.PAYMENT
import dev.crashteam.keanalytics.db.model.tables.pojos.Payment
import org.jooq.DSLContext
import org.springframework.stereotype.Repository

@Repository
class PaymentJooqRepository(
    private val dsl: DSLContext
) : PaymentRepository {

    override fun saveNewPayment(payment: Payment) {
        val p = PAYMENT
        dsl.insertInto(
            p,
            p.PAYMENT_ID,
            p.CREATED_AT,
            p.USER_ID,
            p.STATUS,
            p.PAID,
            p.AMOUNT,
            p.SUBSCRIPTION_TYPE,
            p.MULTIPLY,
        ).values(
            payment.paymentId,
            payment.createdAt,
            payment.userId,
            payment.status,
            payment.paid,
            payment.amount,
            payment.subscriptionType,
            payment.multiply
        ).onConflictDoNothing().execute()
    }

    override fun updatePaymentStatus(paymentId: String, paymentStatus: String, paid: Boolean): Int {
        val p = PAYMENT
        return dsl.update(p)
            .set(p.STATUS, paymentStatus)
            .set(p.PAID, paid)
            .where(p.PAYMENT_ID.eq(paymentId))
            .execute()
    }

    override fun findByPaymentId(paymentId: String): Payment? {
        val p = PAYMENT
        return dsl.selectFrom(p)
            .where(p.PAYMENT_ID.eq(paymentId))
            .fetchOneInto(Payment::class.java)
    }
}
