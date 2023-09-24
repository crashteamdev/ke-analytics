package dev.crashteam.keanalytics.calculator

import dev.crashteam.keanalytics.domain.mongo.AdvancedSubscription
import dev.crashteam.keanalytics.domain.mongo.ProSubscription
import dev.crashteam.keanalytics.repository.mongo.ReferralCodeRepository
import dev.crashteam.keanalytics.repository.mongo.UserRepository
import kotlinx.coroutines.reactor.awaitSingleOrNull
import mu.KotlinLogging
import java.math.BigDecimal

private val log = KotlinLogging.logger {}

class ReferralCodePriceCalculator(
    private val priceCalculatorOption: PriceCalculatorOption,
    private val userRepository: UserRepository,
    private val referralCodeRepository: ReferralCodeRepository,
) : PriceCalculator {

    override suspend fun calculatePrice(): BigDecimal {
        val priceCalculatorContext = priceCalculatorOption.context
        val userReferralCode = referralCodeRepository.findByUserId(priceCalculatorContext.userId).awaitSingleOrNull()
        val inviteReferralCode =
            referralCodeRepository.findByCode(priceCalculatorOption.referralCode!!).awaitSingleOrNull()
        val userDocument = userRepository.findByUserId(priceCalculatorContext.userId).awaitSingleOrNull()
        log.info { "User referral code vs request code. ${userReferralCode?.code}=${priceCalculatorOption.referralCode}" }
        val isUserCanUseReferral = inviteReferralCode != null &&
                userReferralCode?.code != inviteReferralCode.code &&
                userDocument?.subscription == null
        log.info { "IsUserCanUseReferral code: $isUserCanUseReferral" }
        val subscriptionPrice = priceCalculatorContext.subscription.price()
        val amount = if (priceCalculatorContext.multiply > 1) {
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
        } else if (priceCalculatorOption.referralCode.isNotBlank()) {
            if (isUserCanUseReferral) {
                (BigDecimal(subscriptionPrice) - (BigDecimal(subscriptionPrice) * BigDecimal(
                    0.15
                ))).toLong().toString()
            } else subscriptionPrice.toString()
        } else {
            subscriptionPrice.toString()
        }

        return amount.toBigDecimal()
    }
}
