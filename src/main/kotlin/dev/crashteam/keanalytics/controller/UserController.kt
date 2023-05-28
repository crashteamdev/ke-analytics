package dev.crashteam.keanalytics.controller

import kotlinx.coroutines.reactor.awaitSingleOrNull
import mu.KotlinLogging
import dev.crashteam.keanalytics.controller.model.ReferralCodeView
import dev.crashteam.keanalytics.controller.model.UserApiKey
import dev.crashteam.keanalytics.controller.model.UserSubscriptionView
import dev.crashteam.keanalytics.extensions.mapToUserSubscription
import dev.crashteam.keanalytics.repository.mongo.UserRepository
import dev.crashteam.keanalytics.service.UserService
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken
import org.springframework.web.bind.annotation.*
import java.security.Principal
import java.time.LocalDateTime

private val log = KotlinLogging.logger {}

@RestController
@RequestMapping(path = ["v1/user"], produces = [MediaType.APPLICATION_JSON_VALUE])
class UserController(
    private val userService: UserService,
    private val userRepository: UserRepository,
) {

    @PostMapping("/api-key")
    suspend fun createApiKey(principal: Principal): ResponseEntity<UserApiKey> {
        log.info { "Create apiKey. User=${principal.name}" }
        val email = (principal as JwtAuthenticationToken).token.claims["email"].toString()
        val apiKey = userService.createApiKey(principal.name, email)

        return ResponseEntity.ok(UserApiKey(apiKey.hashKey))
    }

    @GetMapping("/api-key")
    suspend fun getApiKey(principal: Principal): ResponseEntity<UserApiKey> {
        log.info { "Get apiKey. User=${principal.name}" }
        var apiKey = userService.getApiKey(principal.name)
        if (apiKey == null) {
            // Trying to find user by email (Auth0 to Firebase migration)
            val email = (principal as JwtAuthenticationToken).token.claims["email"].toString()
            val userId = principal.token.claims["user_id"].toString()
            apiKey = userService.findByEmailAndChangeUserId(email, userId)?.apiKey
        }
        if (apiKey == null) return ResponseEntity.notFound().build()

        return ResponseEntity.ok(UserApiKey(apiKey.hashKey))
    }

    @PutMapping("/api-key")
    suspend fun updateApiKey(principal: Principal): ResponseEntity<UserApiKey> {
        log.info { "Update apiKey. User=${principal.name}" }
        val apiKey = userService.recreateApiKey(principal.name) ?: return ResponseEntity.notFound().build()

        return ResponseEntity.ok(UserApiKey(apiKey.hashKey))
    }

    @GetMapping("/subscription")
    suspend fun getSubscription(principal: Principal): ResponseEntity<UserSubscriptionView> {
        log.info { "Get user subscription. User=${principal.name}" }
        var user = userService.findUser(principal.name)
        if (user == null) {
            // Trying to find user by email (Auth0 to Firebase migration)
            val email = (principal as JwtAuthenticationToken).token.claims["email"].toString()
            val userId = principal.token.claims["user_id"].toString()
            user = userService.findByEmailAndChangeUserId(email, userId)
        }
        if (user?.subscription == null) {
            return ResponseEntity.notFound().build()
        }

        val sub = UserSubscriptionView(
            user.subscription!!.endAt.compareTo(LocalDateTime.now()) >= 1,
            user.subscription!!.createdAt,
            user.subscription!!.endAt,
            user.subscription!!.subType,
            user.subscription!!.mapToUserSubscription()!!.num
        )
        return ResponseEntity.ok(sub)
    }

    @GetMapping("/subscription/apikey")
    suspend fun getSubscriptionWithApiKey(
        @RequestHeader(name = "X-API-KEY", required = true) apiKey: String,
    ): ResponseEntity<UserSubscriptionView> {
        log.info { "Get user subscription by api key" }
        val user = userRepository.findByApiKey_HashKey(apiKey).awaitSingleOrNull()
        if (user?.subscription == null) {
            return ResponseEntity.notFound().build()
        }

        val sub = UserSubscriptionView(
            user.subscription.endAt > LocalDateTime.now(),
            user.subscription.createdAt,
            user.subscription.endAt,
            user.subscription.subType,
            user.subscription.mapToUserSubscription()!!.num
        )
        return ResponseEntity.ok(sub)
    }

    @PostMapping("/referral-code")
    suspend fun createReferralCode(principal: Principal): ResponseEntity<ReferralCodeView> {
        log.info { "Create referral code. User=${principal.name}" }
        val email = (principal as JwtAuthenticationToken).token.claims["email"].toString()
        val referralCodeDocument = userService.createReferralCode(principal.name, email)

        return ResponseEntity(ReferralCodeView(referralCodeDocument.code), HttpStatus.CREATED)
    }

    @GetMapping("/referral-code")
    suspend fun getReferralCode(principal: Principal): ResponseEntity<ReferralCodeView> {
        log.info { "Get referral code. User=${principal.name}" }
        val referralCode = userService.getUserPromoCode(principal.name) ?: return ResponseEntity.notFound().build()
        return ResponseEntity(ReferralCodeView(referralCode.code), HttpStatus.OK)
    }
}
