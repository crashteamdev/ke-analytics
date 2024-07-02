package dev.crashteam.keanalytics.controller

import dev.crashteam.keanalytics.controller.model.UserApiKey
import dev.crashteam.keanalytics.controller.model.UserSubscriptionView
import dev.crashteam.keanalytics.extensions.mapToUserSubscription
import dev.crashteam.keanalytics.repository.postgres.UserRepository
import dev.crashteam.keanalytics.service.UserService
import mu.KotlinLogging
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken
import org.springframework.web.bind.annotation.*
import java.security.Principal
import java.time.LocalDateTime

private val log = KotlinLogging.logger {}

@RestController
@RequestMapping(path = ["v1"], produces = [MediaType.APPLICATION_JSON_VALUE])
class UserController(
    private val userService: UserService,
    private val userRepository: UserRepository,
) {

    @PostMapping("/user/api-key")
    suspend fun createApiKey(principal: Principal): ResponseEntity<UserApiKey> {
        log.debug { "Create apiKey. User=${principal.name}" }
        val email = (principal as JwtAuthenticationToken).token.claims["email"].toString()
        val apiKey = userService.createApiKey(principal.name, email)

        return ResponseEntity.ok(UserApiKey(apiKey.hashKey))
    }

    @GetMapping("/user/api-key")
    suspend fun getApiKey(principal: Principal): ResponseEntity<UserApiKey> {
        log.debug { "Get apiKey. User=${principal.name}" }
        val apiKey = userService.getApiKey(principal.name) ?: return ResponseEntity.notFound().build()

        return ResponseEntity.ok(UserApiKey(apiKey.hashKey))
    }

    @PutMapping("/user/api-key")
    suspend fun updateApiKey(principal: Principal): ResponseEntity<UserApiKey> {
        log.debug { "Update apiKey. User=${principal.name}" }
        val apiKey = userService.recreateApiKey(principal.name) ?: return ResponseEntity.notFound().build()

        return ResponseEntity.ok(UserApiKey(apiKey.hashKey))
    }

    @GetMapping("/user/subscription")
    suspend fun getSubscription(principal: Principal): ResponseEntity<UserSubscriptionView> {
        log.debug { "Get user subscription. User=${principal.name}" }
        val user = userService.findUser(principal.name)
        if (user?.subscriptionType == null) {
            return ResponseEntity.notFound().build()
        }

        val sub = UserSubscriptionView(
            user.subscriptionEndAt.compareTo(LocalDateTime.now()) >= 1,
            user.subscriptionCreatedAt,
            user.subscriptionEndAt,
            user.subscriptionType.literal,
            user.subscriptionType.mapToUserSubscription().num
        )
        return ResponseEntity.ok(sub)
    }


    @GetMapping("/user/subscription/apikey")
    suspend fun getSubscriptionWithApiKey(
        @RequestHeader(name = "X-API-KEY", required = true) apiKey: String,
    ): ResponseEntity<UserSubscriptionView> {
        val user = userRepository.findByApiKey_HashKey(apiKey)
        if (user?.subscriptionType == null) {
            return ResponseEntity.notFound().build()
        }

        val sub = UserSubscriptionView(
            user.subscriptionEndAt > LocalDateTime.now(),
            user.subscriptionCreatedAt,
            user.subscriptionEndAt,
            user.subscriptionType.literal,
            user.subscriptionType.mapToUserSubscription().num
        )
        return ResponseEntity.ok(sub)
    }
}
