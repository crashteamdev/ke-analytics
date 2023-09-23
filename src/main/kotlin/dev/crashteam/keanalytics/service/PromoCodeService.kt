package dev.crashteam.keanalytics.service

import dev.crashteam.keanalytics.domain.mongo.PromoCodeDocument
import dev.crashteam.keanalytics.promo.PromoCodeConfig
import dev.crashteam.keanalytics.promo.PromoCodeGenerator
import dev.crashteam.keanalytics.repository.mongo.PromoCodeRepository
import dev.crashteam.keanalytics.service.model.PromoCodeCreateData
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono

@Service
class PromoCodeService(
    private val promoCodeGenerator: PromoCodeGenerator,
    private val promoCodeRepository: PromoCodeRepository,
) {

    fun createPromoCode(promoCodeCreateData: PromoCodeCreateData): Mono<PromoCodeDocument> {
        val promoCodeConfig = if (promoCodeCreateData.prefix != null) {
            PromoCodeConfig.prefix(promoCodeCreateData.prefix)
        } else {
            PromoCodeConfig.length(7)
        }
        val promoCode = promoCodeGenerator.generate(promoCodeConfig)
        val promoCodeDocument = PromoCodeDocument(
            code = promoCode,
            description = promoCodeCreateData.description,
            validUntil = promoCodeCreateData.validUntil,
            useLimit = promoCodeCreateData.useLimit,
            type = promoCodeCreateData.type,
            discount = promoCodeCreateData.discount,
            additionalDays = promoCodeCreateData.additionalDays
        )
        return promoCodeRepository.save(promoCodeDocument)
    }
}
