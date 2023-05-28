package dev.crashteam.keanalytics.generator

import org.springframework.stereotype.Component
import java.security.SecureRandom
import java.util.*

@Component
class ReferralCodeGenerator {

    private val random = SecureRandom()

    fun generate(): String {
        val sb = StringBuilder()
        val chars: CharArray = ALPHANUMERIC.toCharArray()
        val charsPattern = CharArray(8)
        Arrays.fill(charsPattern, '#')
        val pattern = String(charsPattern)
        for (i in pattern.indices) {
            if (pattern[i] == '#') {
                sb.append(chars[random.nextInt(chars.size)])
            } else {
                sb.append(pattern[i])
            }
        }
        return sb.toString()
    }

    companion object {
        const val ALPHANUMERIC = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
    }

}
