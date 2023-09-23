package dev.crashteam.keanalytics.promo

import org.springframework.stereotype.Component
import kotlin.random.Random

@Component
class PromoCodeGenerator {

    fun generate(promoCodeConfig: PromoCodeConfig): String {
        val random = Random(System.currentTimeMillis())
        val sb = StringBuilder()
        val chars: CharArray = promoCodeConfig.charset.toCharArray()
        val pattern: CharArray = promoCodeConfig.pattern.toCharArray()
        if (promoCodeConfig.prefix != null) {
            sb.append(promoCodeConfig.prefix)
        }
        for (i in pattern.indices) {
            if (pattern[i] == PromoCodeConfig.PATTERN_PLACEHOLDER) {
                sb.append(chars[random.nextInt(chars.size)])
            } else {
                sb.append(pattern[i])
            }
        }
        return sb.toString()
    }
}
