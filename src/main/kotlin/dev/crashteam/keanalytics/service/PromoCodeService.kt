package dev.crashteam.keanalytics.service

import dev.crashteam.keanalytics.domain.mongo.PromoCodeDocument
import dev.crashteam.keanalytics.promo.PromoCodeConfig
import dev.crashteam.keanalytics.promo.PromoCodeGenerator
import dev.crashteam.keanalytics.repository.mongo.PromoCodeRepository
import dev.crashteam.keanalytics.service.model.PromoCodeCheckCode
import dev.crashteam.keanalytics.service.model.PromoCodeCheckResult
import dev.crashteam.keanalytics.service.model.PromoCodeCreateData
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toMono
import java.time.LocalDateTime

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
            additionalDays = promoCodeCreateData.additionalDays,
            numberOfUses = 0
        )
        return promoCodeRepository.save(promoCodeDocument)
    }

    fun checkPromoCode(promoCode: String): Mono<PromoCodeCheckResult> {
        val promoCodeDocumentMono = promoCodeRepository.findByCode(promoCode)
        return promoCodeDocumentMono.flatMap { promoCodeDocument ->
            val promoCodeCheckResult = if (promoCodeDocument.numberOfUses >= promoCodeDocument.useLimit) {
                PromoCodeCheckResult(
                    PromoCodeCheckCode.INVALID_USE_LIMIT,
                    "The promo code has already been used more times than the limit has been set"
                )
            } else if (promoCodeDocument.validUntil < LocalDateTime.now()) {
                PromoCodeCheckResult(
                    PromoCodeCheckCode.INVALID_DATE_LIMIT,
                    "The promo code is outdated"
                )
            } else {
                PromoCodeCheckResult(
                    PromoCodeCheckCode.VALID,
                    "Valid promo code"
                )
            }
            promoCodeCheckResult.toMono()
        }.switchIfEmpty(Mono.defer {
            PromoCodeCheckResult(
                PromoCodeCheckCode.NOT_FOUND,
                "Promo code not found"
            ).toMono()
        })
    }
}
