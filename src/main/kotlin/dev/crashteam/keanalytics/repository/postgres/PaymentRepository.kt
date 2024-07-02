package dev.crashteam.keanalytics.repository.postgres

import dev.crashteam.keanalytics.db.model.tables.pojos.Payment

interface PaymentRepository {
    fun saveNewPayment(payment: Payment)

    fun updatePaymentStatus(paymentId: String, paymentStatus: String, paid: Boolean): Int

    fun findByPaymentId(paymentId: String): Payment?
}
