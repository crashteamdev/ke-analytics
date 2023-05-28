package dev.crashteam.keanalytics.security

import mu.KotlinLogging
import dev.crashteam.keanalytics.repository.mongo.UserRepository
import org.springframework.http.HttpStatus
import org.springframework.web.server.ServerWebExchange
import org.springframework.web.server.ServerWebInputException
import org.springframework.web.server.WebFilter
import org.springframework.web.server.WebFilterChain
import reactor.core.publisher.Mono
import java.time.LocalDateTime

private val log = KotlinLogging.logger {}

class ApiKeyAuthHandlerFilter(
    private val userRepository: UserRepository,
) : WebFilter {

    override fun filter(exchange: ServerWebExchange, chain: WebFilterChain): Mono<Void> {
        val apiKeyHeaderValue = exchange.request.headers["X-API-KEY"]
        if (apiKeyHeaderValue == null) {
            exchange.response.rawStatusCode = HttpStatus.UNAUTHORIZED.value()
            return exchange.response.setComplete()
        }
        val apiKey: String = apiKeyHeaderValue.single()

        return userRepository.findByApiKey_HashKey(apiKey).doOnSuccess { user ->
            if (user?.apiKey == null || user.apiKey.blocked) {
                throw AuthorizationException("Not valid API key")
            }
//            if (user.subscription == null || user.subscription.endAt <= LocalDateTime.now()) {
//                throw AuthorizationException("No access for user")
//            }
        }.flatMap {
            chain.filter(exchange)
        }.onErrorResume {
            if (it is AuthorizationException) {
                exchange.response.rawStatusCode = HttpStatus.FORBIDDEN.value()
                exchange.response.setComplete()
            } else {
                log.error(it) { "Failed to handle request" }
                val serverWebInputException = it as? ServerWebInputException
                exchange.response.rawStatusCode =
                    serverWebInputException?.status?.value() ?: HttpStatus.INTERNAL_SERVER_ERROR.value()
                exchange.response.setComplete()
            }
        }
    }
}
