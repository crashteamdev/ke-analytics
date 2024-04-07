package dev.crashteam.keanalytics.math

import java.math.BigDecimal

object MathUtils {

    fun percentageDifference(a: BigDecimal, b: BigDecimal): BigDecimal {
        return if (a == BigDecimal.ZERO) {
            if (b == BigDecimal.ZERO) {
                BigDecimal.ZERO
            } else {
                BigDecimal("100")
            }
        } else {
            val difference = b - a
            ((difference / a.abs()) * BigDecimal("100")).stripTrailingZeros()
        }
    }

    fun percentageDifference(a: Long, b: Long): Double {
        return if (a == 0L && b == 0L) {
            0.0
        } else if (a == 0L) {
            0.0
        } else {
            val difference = b - a
            (difference.toDouble() / a) * 100
        }
    }

}
