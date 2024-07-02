package dev.crashteam.keanalytics.security

import dev.crashteam.keanalytics.repository.postgres.UserRepository
import mu.KotlinLogging
import org.springframework.http.HttpStatus
import org.springframework.web.server.ServerWebExchange
import org.springframework.web.server.WebFilter
import org.springframework.web.server.WebFilterChain
import reactor.core.publisher.Mono

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
        val user = userRepository.findByApiKey_HashKey(apiKey)
        if (user == null) {
            exchange.response.rawStatusCode = HttpStatus.FORBIDDEN.value()
            return exchange.response.setComplete()
        }
        return chain.filter(exchange)
    }
}
