package dev.crashteam.keanalytics.calculator

import dev.crashteam.keanalytics.repository.mongo.PromoCodeRepository
import dev.crashteam.keanalytics.repository.mongo.ReferralCodeRepository
import dev.crashteam.keanalytics.repository.mongo.UserRepository
import org.springframework.stereotype.Component

@Component
class PriceCalculatorFactory(
    private val userRepository: UserRepository,
    private val referralCodeRepository: ReferralCodeRepository,
    private val promoCodeRepository: PromoCodeRepository,
) {

    fun createPriceCalculator(priceCalculatorOption: PriceCalculatorOption): PriceCalculator {
        return if (priceCalculatorOption.promoCode != null) {
            PromoCodePriceCalculator(promoCodeRepository)
        } else if (priceCalculatorOption.referralCode != null) {
            ReferralCodePriceCalculator(userRepository, referralCodeRepository)
        } else {
            SimplePriceCalculator()
        }
    }
}
