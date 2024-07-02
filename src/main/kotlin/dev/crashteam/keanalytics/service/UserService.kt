package dev.crashteam.keanalytics.service

import com.google.common.hash.Hashing
import dev.crashteam.keanalytics.db.model.tables.pojos.Users
import dev.crashteam.keanalytics.domain.ApiKey
import dev.crashteam.keanalytics.repository.postgres.UserRepository
import dev.crashteam.keanalytics.service.exception.ApiKeyAlreadyExists
import org.springframework.stereotype.Service
import java.nio.charset.Charset
import java.util.*

@Service
class UserService(
    val userRepository: UserRepository,
) {

    suspend fun findUser(userId: String): Users? {
        return userRepository.findByUserId(userId)
    }

    suspend fun createApiKey(userId: String, email: String): ApiKey {
        var user = userRepository.findByUserId(userId)
        if (user?.apiKeyKey != null) {
            throw ApiKeyAlreadyExists("API key already exists for user=$userId")
        }

        val apiKey = generateApiKey()
        if (user == null) {
            user = Users().apply {
                this.userId = userId
                this.apiKeyKey = apiKey.key
                this.apiKeyPrefix = apiKey.prefix
                this.apiKeyHashKey = apiKey.hashKey
                this.apiKeyBlocked = apiKey.blocked
                this.email = email
            }
            userRepository.save(user)
        } else {
            userRepository.updateApiKey(userId, apiKey)
        }

        return apiKey
    }

    suspend fun recreateApiKey(userId: String): ApiKey? {
        userRepository.findByUserId(userId) ?: return null
        val apiKey = generateApiKey()
        userRepository.updateApiKey(userId, apiKey)

        return apiKey
    }

    suspend fun getApiKey(userId: String): ApiKey? {
        val user = userRepository.findByUserId(userId) ?: return null
        return ApiKey(
            prefix = user.apiKeyPrefix,
            key = user.apiKeyKey,
            hashKey = user.apiKeyHashKey,
            blocked = user.apiKeyBlocked
        )
    }

    private fun generateApiKey(): ApiKey {
        val prefix: String = generateRandomString(8)
        val key: String = generateRandomString(35)
        val newApiKeyVal = "$prefix.$key"
        val hashKey = prefix + "." + Hashing.sha256().hashString(newApiKeyVal, Charset.defaultCharset())

        return ApiKey(prefix, key, hashKey, false)
    }

    private fun generateRandomString(maxLength: Int): String {
        return Random().ints(48, 122)
            .filter { i: Int -> (i < 57 || i > 65) && (i < 90 || i > 97) }
            .mapToObj { i: Int -> i.toChar() }
            .limit(maxLength.toLong())
            .collect(
                { StringBuilder() },
                StringBuilder::append,
                StringBuilder::append
            )
            .toString()
    }
}
