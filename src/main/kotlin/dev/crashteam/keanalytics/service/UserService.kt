package dev.crashteam.keanalytics.service

import com.google.common.hash.Hashing
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import dev.crashteam.keanalytics.domain.mongo.ApiKey
import dev.crashteam.keanalytics.domain.mongo.ReferralCodeDocument
import dev.crashteam.keanalytics.domain.mongo.UserDocument
import dev.crashteam.keanalytics.generator.ReferralCodeGenerator
import dev.crashteam.keanalytics.repository.mongo.ReferralCodeRepository
import dev.crashteam.keanalytics.repository.mongo.UserRepository
import dev.crashteam.keanalytics.service.exception.ApiKeyAlreadyExists
import org.springframework.data.mongodb.core.ReactiveMongoTemplate
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.Update
import org.springframework.stereotype.Service
import java.nio.charset.Charset
import java.util.*

@Service
class UserService(
    val userRepository: UserRepository,
    val referralCodeRepository: ReferralCodeRepository,
    val referralCodeGenerator: ReferralCodeGenerator,
    val reactiveMongoTemplate: ReactiveMongoTemplate
) {

    suspend fun findUser(userId: String): UserDocument? {
        return userRepository.findByUserId(userId).awaitSingleOrNull()
    }

    suspend fun createApiKey(userId: String, email: String): ApiKey {
        var user = userRepository.findByUserId(userId).awaitSingleOrNull()
        if (user?.apiKey != null) {
            throw ApiKeyAlreadyExists("API key already exists for user=$userId")
        }

        val apiKey = generateApiKey()
        if (user == null) {
            user = UserDocument(userId, null, apiKey, email)
        } else {
            user = user.copy(apiKey = apiKey)
        }

        userRepository.save(user).awaitSingle()

        return apiKey
    }

    suspend fun recreateApiKey(userId: String): ApiKey? {
        var user = userRepository.findByUserId(userId).awaitSingleOrNull() ?: return null
        val apiKey = generateApiKey()
        user = user.copy(apiKey = apiKey)
        userRepository.save(user).awaitSingle()

        return apiKey
    }

    suspend fun getApiKey(userId: String): ApiKey? {
        val user = userRepository.findByUserId(userId).awaitSingleOrNull()
        return user?.apiKey
    }

    suspend fun findByEmailAndChangeUserId(email: String, newUserId: String): UserDocument? {
        val users = userRepository.findByEmail(email).collectList().awaitSingleOrNull() ?: return null
        val user: UserDocument? = if (users.size > 1) {
            users.find { it.email?.startsWith("auth0") != true }
        } else if (users.size == 1) {
            users[0]
        } else null

        if (user == null) return null

        val query = Query().apply { addCriteria(Criteria.where("userId").`is`(user.userId)) }
        val update = Update().apply { set("userId", newUserId) }
        val userDocument = reactiveMongoTemplate.findAndModify(query, update, UserDocument::class.java).awaitSingle()

        return userDocument
    }

    suspend fun createReferralCode(userId: String, email: String): ReferralCodeDocument {
        val referralCodeDocument = referralCodeRepository.findByUserId(userId).awaitSingleOrNull()
        if (referralCodeDocument != null) {
            return referralCodeDocument
        }
        val referralCode = referralCodeGenerator.generate()

        return referralCodeRepository.save(ReferralCodeDocument(userId, referralCode, null)).awaitSingle()
    }

    suspend fun getUserPromoCode(userId: String): ReferralCodeDocument? {
        return referralCodeRepository.findByUserId(userId).awaitSingleOrNull()
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
