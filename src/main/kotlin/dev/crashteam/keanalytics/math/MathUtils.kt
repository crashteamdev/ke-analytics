package dev.crashteam.keanalytics.math

import java.math.BigDecimal

object MathUtils {

    fun percentageDifference(a: Long, b: Long): Long {
        return if (a > b) {
            -((a/b - 1) * 100)
        } else if (a < b) {
            (b/a - 1) * 100
        } else 0
    }

    fun percentageDifference(a: BigDecimal, b: BigDecimal): BigDecimal {
        return if (a > b) {
            ((a/b - BigDecimal.ONE) * BigDecimal.valueOf(100)).negate()
        } else if (a < b) {
            (b/a - BigDecimal.ONE) * BigDecimal.valueOf(100)
        } else BigDecimal.ZERO
    }

}
