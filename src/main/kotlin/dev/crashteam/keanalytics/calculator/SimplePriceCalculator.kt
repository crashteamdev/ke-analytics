package dev.crashteam.keanalytics.calculator

import dev.crashteam.keanalytics.domain.mongo.AdvancedSubscription
import dev.crashteam.keanalytics.domain.mongo.ProSubscription
import java.math.BigDecimal

class SimplePriceCalculator(
    private val priceCalculatorOption: PriceCalculatorOption,
) : PriceCalculator {

    override suspend fun calculatePrice(): BigDecimal {
        val priceCalculatorContext = priceCalculatorOption.context
        val subscriptionPrice = priceCalculatorContext.subscription.price()
        if (priceCalculatorContext.multiply > 1) {
            val multipliedAmount =
                BigDecimal(subscriptionPrice) * BigDecimal.valueOf(priceCalculatorContext.multiply.toLong())
            val discount = if (priceCalculatorContext.multiply == 3.toShort()) {
                when (priceCalculatorContext.subscription) {
                    AdvancedSubscription -> {
                        BigDecimal(0.15)
                    }

                    ProSubscription -> {
                        BigDecimal(0.20)
                    }

                    else -> BigDecimal(0.10)
                }
            } else if (priceCalculatorContext.multiply >= 6) {
                BigDecimal(0.30)
            } else BigDecimal(0.10)
            (multipliedAmount - (multipliedAmount * discount)).toLong().toString()

            return priceCalculatorOption.context.subscription.price().toBigDecimal()
        } else {
            return priceCalculatorOption.context.subscription.price().toBigDecimal()
        }
    }
}
