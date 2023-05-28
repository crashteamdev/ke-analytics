package dev.crashteam.keanalytics.config

import dev.crashteam.keanalytics.repository.mongo.UserRepository
import dev.crashteam.keanalytics.repository.redis.ApiKeyUserSessionInfo
import dev.crashteam.keanalytics.security.ApiKeyAuthHandlerFilter
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.core.annotation.Order
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.http.HttpMethod
import org.springframework.http.HttpStatus
import org.springframework.security.authentication.BadCredentialsException
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken
import org.springframework.security.config.annotation.method.configuration.EnableReactiveMethodSecurity
import org.springframework.security.config.annotation.web.reactive.EnableWebFluxSecurity
import org.springframework.security.config.web.server.SecurityWebFiltersOrder
import org.springframework.security.config.web.server.ServerHttpSecurity
import org.springframework.security.core.Authentication
import org.springframework.security.oauth2.core.DelegatingOAuth2TokenValidator
import org.springframework.security.oauth2.core.OAuth2TokenValidator
import org.springframework.security.oauth2.jwt.*
import org.springframework.security.oauth2.server.resource.authentication.JwtGrantedAuthoritiesConverter
import org.springframework.security.oauth2.server.resource.authentication.ReactiveJwtAuthenticationConverter
import org.springframework.security.oauth2.server.resource.authentication.ReactiveJwtGrantedAuthoritiesConverterAdapter
import org.springframework.security.web.server.SecurityWebFilterChain
import org.springframework.security.web.server.util.matcher.ServerWebExchangeMatchers.pathMatchers
import org.springframework.web.cors.CorsConfiguration
import org.springframework.web.cors.reactive.CorsConfigurationSource
import org.springframework.web.cors.reactive.UrlBasedCorsConfigurationSource
import reactor.core.publisher.Mono

@EnableWebFluxSecurity
@EnableReactiveMethodSecurity
class SecurityConfig(
    val userRepository: UserRepository,
) {

    @Value("\${spring.security.oauth2.resourceserver.jwt.issuer-uri}")
    private lateinit var issuer: String

//    @PostConstruct
//    fun trustAll() {
//        val trm: TrustManager = object : X509TrustManager {
//            override fun checkClientTrusted(certs: Array<X509Certificate>, authType: String?) {}
//            override fun checkServerTrusted(certs: Array<X509Certificate>, authType: String?) {}
//            override fun getAcceptedIssuers(): Array<X509Certificate>? {
//                return null
//            }
//        }
//        val sc = SSLContext.getInstance("TLS")
//        sc.init(null, arrayOf(trm), null)
//        HttpsURLConnection.setDefaultSSLSocketFactory(sc.socketFactory)
//        HttpsURLConnection.setDefaultHostnameVerifier { _, _ -> true }
//    }

    @Bean
    @Order(1)
    fun basicAuthWebFilterChain(http: ServerHttpSecurity): SecurityWebFilterChain {
        return http
            .cors().configurationSource(createCorsConfigSource()).and()
            .securityMatcher(pathMatchers("/actuator/**")).anonymous().and()
            .authorizeExchange { spec ->
                run {
                    spec.pathMatchers("/actuator/**").permitAll()
                }
            }
            .build()
    }

    @Bean
    @Order(2)
    fun apiKeyWebFilterChain(http: ServerHttpSecurity): SecurityWebFilterChain {
        return http
            .cors().configurationSource(createCorsConfigSource()).and()
            .securityMatcher(
                pathMatchers(
                    "/v1/product/**",
                    "/v1/seller/**",
                    "/v1/categories/**",
                    "/v1/category/**",
                    "/v1/report/**",
                    "/v1/reports/**",
                    "/v1/user/subscription/apikey",
                    "/v1/similar/**",
                    "/v2/category/**",
                    "/v2/report/**",
                    "/v2/reports/**",
                    "/v2/seller/**",
                    "/v2/product/**",
                    "/v2/seller/**",
                )
            )
            .addFilterAt(ApiKeyAuthHandlerFilter(userRepository), SecurityWebFiltersOrder.AUTHORIZATION)
            //.addFilterAt(ApiUserLimiterFilter(apiKeySessionRedisTemplate), SecurityWebFiltersOrder.LAST)
            .build()
    }

    @Bean
    @Order(3)
    fun oAuthWebFilterChain(http: ServerHttpSecurity): SecurityWebFilterChain {
        return http
            .cors().configurationSource(createCorsConfigSource()).and()
            .authorizeExchange().anyExchange().authenticated().and()
            .oauth2ResourceServer()
            .jwt()
            .jwtDecoder(jwtDecoder())
            .jwtAuthenticationConverter(jwtAuthenticationConverter()).and()
            .and()
            .build()
    }

    @Bean
    fun jwtDecoder(): ReactiveJwtDecoder {
        val jwtDecoder = ReactiveJwtDecoders.fromOidcIssuerLocation(issuer) as NimbusReactiveJwtDecoder
        val withIssuer = JwtValidators.createDefaultWithIssuer(issuer)
        val withAudience: OAuth2TokenValidator<Jwt> = DelegatingOAuth2TokenValidator(
            withIssuer,
            JwtTimestampValidator()
        )
        jwtDecoder.setJwtValidator(withAudience)

        return jwtDecoder
    }

    fun jwtAuthenticationConverter(): ReactiveJwtAuthenticationConverter {
        val converter = JwtGrantedAuthoritiesConverter()
        converter.setAuthoritiesClaimName("permissions")
        converter.setAuthorityPrefix("")
        val reactiveJwtGrantedAuthoritiesConverterAdapter = ReactiveJwtGrantedAuthoritiesConverterAdapter(converter)
        val reactiveJwtAuthenticationConverter = ReactiveJwtAuthenticationConverter()
        reactiveJwtAuthenticationConverter.setJwtGrantedAuthoritiesConverter(
            reactiveJwtGrantedAuthoritiesConverterAdapter
        )

        return reactiveJwtAuthenticationConverter
    }

    private fun createCorsConfigSource(): CorsConfigurationSource {
        val source = UrlBasedCorsConfigurationSource()
        val config = CorsConfiguration()
        config.applyPermitDefaultValues()
        config.addAllowedMethod(HttpMethod.PUT)
        config.allowCredentials = true
        config.allowedOrigins = null
        config.allowedOriginPatterns = listOf("*")
        config.addExposedHeader("Authorization")
        source.registerCorsConfiguration("/**", config)
        return source
    }
}
