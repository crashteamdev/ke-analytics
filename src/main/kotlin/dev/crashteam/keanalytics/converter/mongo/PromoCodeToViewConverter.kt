package dev.crashteam.keanalytics.converter.mongo

import dev.crashteam.keanalytics.converter.DataConverter
import dev.crashteam.keanalytics.domain.mongo.PromoCodeDocument
import dev.crashteam.keanalytics.domain.mongo.PromoCodeType
import dev.crashteam.openapi.keanalytics.model.AdditionalTimePromoCode
import dev.crashteam.openapi.keanalytics.model.DiscountPromoCode
import dev.crashteam.openapi.keanalytics.model.PromoCode
import dev.crashteam.openapi.keanalytics.model.PromoCodeContext
import org.springframework.stereotype.Component
import java.time.ZoneOffset

@Component
class PromoCodeToViewConverter : DataConverter<PromoCodeDocument, PromoCode> {

    override fun convert(source: PromoCodeDocument): PromoCode {
        val promoCodeContext: PromoCodeContext = when (source.type) {
            PromoCodeType.DISCOUNT -> {
                val discountPromoCode = DiscountPromoCode(source.discount!!.toInt())
                discountPromoCode.type = PromoCodeContext.TypeEnum.DISCOUNT
                discountPromoCode
            }
            PromoCodeType.ADDITIONAL_DAYS -> {
                val additionalTimePromoCode = AdditionalTimePromoCode(source.additionalDays)
                additionalTimePromoCode.type = PromoCodeContext.TypeEnum.ADDITIONAL_TIME
                additionalTimePromoCode
            }
        }
        return PromoCode(
            source.code,
            source.description,
            source.validUntil.atOffset(ZoneOffset.UTC),
            source.useLimit,
            promoCodeContext
        )
    }
}
