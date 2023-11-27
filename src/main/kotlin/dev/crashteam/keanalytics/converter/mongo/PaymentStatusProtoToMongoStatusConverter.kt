package dev.crashteam.keanalytics.converter.mongo

import dev.crashteam.payment.PaymentStatus
import org.springframework.core.convert.converter.Converter
import org.springframework.stereotype.Component

@Component
class PaymentStatusProtoToMongoStatusConverter : Converter<PaymentStatus, String> {
    override fun convert(source: PaymentStatus): String? {
        return when (source) {
            PaymentStatus.PAYMENT_STATUS_PENDING -> "pending"
            PaymentStatus.PAYMENT_STATUS_SUCCESS -> "success"
            PaymentStatus.PAYMENT_STATUS_CANCELED -> "canceled"
            PaymentStatus.PAYMENT_STATUS_FAILED -> "failed"
            PaymentStatus.PAYMENT_STATUS_UNKNOWN, PaymentStatus.UNRECOGNIZED -> "unknown"
        }
    }
}
