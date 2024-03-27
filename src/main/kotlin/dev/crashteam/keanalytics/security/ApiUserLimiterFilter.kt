package dev.crashteam.keanalytics.security

import kotlinx.coroutines.reactor.awaitSingleOrNull
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import dev.crashteam.keanalytics.repository.redis.ApiKeyAccessFrom
import dev.crashteam.keanalytics.repository.redis.ApiKeyUserSessionInfo
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.web.server.ServerWebExchange
import org.springframework.web.server.WebFilter
import org.springframework.web.server.WebFilterChain
import reactor.core.publisher.Mono
import java.time.Duration
import java.time.temporal.ChronoUnit

private val log = KotlinLogging.logger {}

class ApiUserLimiterFilter(
    private val reactiveRedisTemplate: ReactiveRedisTemplate<String, ApiKeyUserSessionInfo>
) : WebFilter {

    override fun filter(exchange: ServerWebExchange, chain: WebFilterChain): Mono<Void> {
        val ip =
            exchange.request.headers["CF-Connecting-IP"]?.single()
                ?: exchange.request.headers["X-Real-IP"]?.single()
                ?: "unknown"
        val browser = exchange.request.headers[HttpHeaders.USER_AGENT]?.single()
        val apiKey = exchange.request.headers["X-API-KEY"]?.single()!!
        val sessionInfo = runBlocking {
            val userSessionInfo = reactiveRedisTemplate.opsForValue().get(apiKey).awaitSingleOrNull()
            if (userSessionInfo == null) {
                val sessionInfo = ApiKeyUserSessionInfo().apply {
                    val apiKeyAccessFrom = ApiKeyAccessFrom().apply {
                        this.ip = ip
                        this.browser = browser
                    }
                    accessFrom = listOf(apiKeyAccessFrom)
                }
                reactiveRedisTemplate.opsForValue().set(apiKey, sessionInfo).awaitSingleOrNull()?.let {
                    val expire = reactiveRedisTemplate.getExpire(apiKey).awaitSingleOrNull()
                    if (expire?.isZero == true) {
                        reactiveRedisTemplate.expire(apiKey, Duration.of(3, ChronoUnit.HOURS)).awaitSingleOrNull()
                    }
                }
                return@runBlocking sessionInfo
            } else {
                val accessFrom = userSessionInfo.accessFrom?.find {
                    it.ip == ip && it.browser == browser
                }
                if (accessFrom == null) {
                    // Add new access
                    val newApiKeyUserSessionInfo = ApiKeyUserSessionInfo().apply {
                        val accessFromList = userSessionInfo.accessFrom!!.toMutableList()
                        accessFromList.add(ApiKeyAccessFrom().apply {
                            this.ip = ip
                            this.browser = browser
                        })
                        this.accessFrom = accessFromList
                    }
                    reactiveRedisTemplate.opsForValue().set(apiKey, newApiKeyUserSessionInfo).awaitSingleOrNull()?.let {
                        val expire = reactiveRedisTemplate.getExpire(apiKey).awaitSingleOrNull()
                        if (expire?.isZero == true) {
                            reactiveRedisTemplate.expire(apiKey, Duration.of(3, ChronoUnit.HOURS))
                                .awaitSingleOrNull()
                        }
                    }
                    return@runBlocking newApiKeyUserSessionInfo
                } else {
                    return@runBlocking userSessionInfo
                }
            }
        }
        if (sessionInfo.accessFrom!!.size > 2) {
            val groupByIpMap: Map<String, List<ApiKeyAccessFrom>> = sessionInfo.accessFrom!!.groupBy { it.ip!! }
            if (groupByIpMap.size > 6) {
                log.info { "User have too match sessions from different ip address. ips=${groupByIpMap.keys}" }
                exchange.response.rawStatusCode = HttpStatus.TOO_MANY_REQUESTS.value()
                return exchange.response.setComplete()
            }
            var tooMatchBrowserFromOneIp = false
            for (entry in groupByIpMap) {
                if (entry.value.size > 3) {
                    tooMatchBrowserFromOneIp = true
                }
            }
            if (tooMatchBrowserFromOneIp) {
                log.info { "User have too match sessions from one ip. browsers=${groupByIpMap.values}" }
                exchange.response.rawStatusCode = HttpStatus.TOO_MANY_REQUESTS.value()
                return exchange.response.setComplete()
            }
        }
        return chain.filter(exchange)
    }

}
