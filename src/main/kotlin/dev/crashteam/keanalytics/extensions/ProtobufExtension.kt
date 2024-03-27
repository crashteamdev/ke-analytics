package dev.crashteam.keanalytics.extensions

import com.google.protobuf.Timestamp
import dev.crashteam.keanalytics.model.DatePeriodLocalDate
import dev.crashteam.keanalytics.repository.clickhouse.model.SortOrder
import dev.crashteam.mp.base.Date
import dev.crashteam.mp.base.DatePeriod
import dev.crashteam.mp.base.Money
import dev.crashteam.mp.base.Sort
import java.math.BigDecimal
import java.math.RoundingMode
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneOffset

fun Timestamp.toLocalDateTime(): LocalDateTime {
    return LocalDateTime.ofEpochSecond(
        this.seconds,
        this.nanos,
        ZoneOffset.UTC,
    )
}

fun Date.toLocalDate(): LocalDate {
    return LocalDate.of(this.year, this.month, this.day)
}

fun LocalDate.toProtobufDate(): Date {
    return Date.newBuilder().apply {
        this.year = this@toProtobufDate.year
        this.month = this@toProtobufDate.month.value
        this.day = this@toProtobufDate.dayOfMonth
    }.build()
}

fun BigDecimal.toMoney(): Money {
    val natural = this.toBigInteger().toLong()
    val fractional = this.remainder(BigDecimal.ONE).setScale(9, RoundingMode.DOWN)
    val nanos = fractional.movePointRight(9).intValueExact()
    return Money.newBuilder().apply {
        this.currencyCode = "643" // TODO: refactor hardcoded currency code
        this.units = natural
        this.nanos = nanos
    }.build()
}

fun Sort.SortOrder.toRepositoryDomain(): SortOrder {
    return when (this) {
        dev.crashteam.mp.base.Sort.SortOrder.SORT_ORDER_UNSPECIFIED,
        dev.crashteam.mp.base.Sort.SortOrder.UNRECOGNIZED -> SortOrder.ASC

        dev.crashteam.mp.base.Sort.SortOrder.SORT_ORDER_ASC -> SortOrder.ASC
        dev.crashteam.mp.base.Sort.SortOrder.SORT_ORDER_DESC -> SortOrder.DESC
    }
}

fun DatePeriod.toLocalDates(): DatePeriodLocalDate {
    return when (this) {
        DatePeriod.DATE_PERIOD_WEEK -> {
            DatePeriodLocalDate(LocalDate.now().minusDays(7), LocalDate.now())
        }
        DatePeriod.DATE_PERIOD_TWO_WEEK -> {
            DatePeriodLocalDate(LocalDate.now().minusDays(14), LocalDate.now())
        }
        DatePeriod.DATE_PERIOD_MONTH -> {
            DatePeriodLocalDate(LocalDate.now().minusDays(30), LocalDate.now())
        }
        DatePeriod.DATE_PERIOD_TWO_MONTH -> {
            DatePeriodLocalDate(LocalDate.now().minusDays(60), LocalDate.now())
        }
        DatePeriod.DATE_PERIOD_UNSPECIFIED, DatePeriod.UNRECOGNIZED -> {
            DatePeriodLocalDate(LocalDate.now().minusDays(7), LocalDate.now())
        }
    }
}
