package dev.crashteam.keanalytics.calculator

import dev.crashteam.keanalytics.domain.mongo.PromoCodeType
import dev.crashteam.keanalytics.repository.mongo.PromoCodeRepository
import kotlinx.coroutines.reactor.awaitSingleOrNull
import java.math.BigDecimal
import java.time.LocalDateTime

class PromoCodePriceCalculator(
    private val priceCalculatorOption: PriceCalculatorOption,
    private val promoCodeRepository: PromoCodeRepository,
) : PriceCalculator {

    override suspend fun calculatePrice(): BigDecimal {
        val context = priceCalculatorOption.context
        val promoCodeDocument = promoCodeRepository.findByCode(priceCalculatorOption.promoCode!!).awaitSingleOrNull()
            ?: return context.subscription.price().toBigDecimal()
        val defaultPrice = context.subscription.price().toBigDecimal() * context.multiply.toLong().toBigDecimal()
        if (promoCodeDocument.validUntil < LocalDateTime.now()) {
            return defaultPrice
        }
        return when (promoCodeDocument.type) {
            PromoCodeType.ADDITIONAL_DAYS -> {
                defaultPrice
            }
            PromoCodeType.DISCOUNT -> {
                if (promoCodeDocument.numberOfUses >= promoCodeDocument.useLimit) {
                    defaultPrice
                } else {
                    val price = context.subscription.price().toBigDecimal() * context.multiply.toLong().toBigDecimal()
                    ((price * promoCodeDocument.discount!!.toLong().toBigDecimal()) / BigDecimal.valueOf(100))
                }
            }
        }
    }
}
