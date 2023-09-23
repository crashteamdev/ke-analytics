package dev.crashteam.keanalytics.calculator

import java.math.BigDecimal

class SimplePriceCalculator(
    private val priceCalculatorOption: PriceCalculatorOption,
) : PriceCalculator {

    override suspend fun calculatePrice(): BigDecimal {
        return priceCalculatorOption.context.subscription.price().toBigDecimal()
    }
}
