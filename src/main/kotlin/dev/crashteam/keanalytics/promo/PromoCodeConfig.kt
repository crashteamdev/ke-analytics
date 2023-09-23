package dev.crashteam.keanalytics.promo

import java.util.*

class PromoCodeConfig(
    length: Int? = null,
    charset: String? = null,
    prefix: String? = null,
    pattern: String? = null,
) {

    object Charset {
        const val ALPHABETIC = "ABCDEFGHIJKLMNPQRSTUVWXYZ"
        const val ALPHANUMERIC = "123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNPQRSTUVWXYZ"
        const val NUMBERS = "0123456789"
    }

    val length: Int
    val charset: String
    val prefix: String?
    val pattern: String

    init {
        var length = length
        var charset = charset
        var pattern = pattern
        if (length == null) {
            length = 8
        }
        if (charset == null) {
            charset = Charset.ALPHANUMERIC
        }
        if (pattern == null) {
            val chars = CharArray(length)
            Arrays.fill(chars, PATTERN_PLACEHOLDER)
            pattern = String(chars)
        }
        this.length = length
        this.charset = charset
        this.prefix = prefix
        this.pattern = pattern
    }

    companion object {
        const val PATTERN_PLACEHOLDER = '#'

        fun length(length: Int): PromoCodeConfig {
            return PromoCodeConfig(length)
        }

        fun prefix(prefix: String): PromoCodeConfig {
            return PromoCodeConfig(prefix = prefix)
        }
    }
}
