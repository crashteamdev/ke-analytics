package dev.crashteam.keanalytics.calculator

import java.math.BigDecimal

interface PriceCalculator {

    suspend fun calculatePrice(): BigDecimal

}
