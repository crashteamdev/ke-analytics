package dev.crashteam.keanalytics.config

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.util.StdDateFormat
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import dev.crashteam.keanalytics.retry.LogRetryListener
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.client.ClientHttpRequestFactory
import org.springframework.http.codec.ServerCodecConfigurer
import org.springframework.http.codec.json.Jackson2JsonDecoder
import org.springframework.http.codec.json.Jackson2JsonEncoder
import org.springframework.retry.backoff.ExponentialBackOffPolicy
import org.springframework.retry.policy.SimpleRetryPolicy
import org.springframework.retry.support.RetryTemplate
import org.springframework.web.client.RestTemplate
import org.springframework.web.reactive.config.PathMatchConfigurer
import org.springframework.web.reactive.config.WebFluxConfigurer


@Configuration
class AppConfig {

    @Bean
    fun restTemplate(
        @Qualifier("simpleHttpRequestFactory") clientHttpRequestFactory: ClientHttpRequestFactory
    ): RestTemplate {
        return RestTemplate(clientHttpRequestFactory)
    }

    @Bean
    fun retryTemplate(): RetryTemplate {
        val retryTemplate = RetryTemplate()
        val exponentialBackOffPolicy = ExponentialBackOffPolicy()
        exponentialBackOffPolicy.initialInterval = 10000L
        exponentialBackOffPolicy.maxInterval = 60000L
        retryTemplate.setBackOffPolicy(exponentialBackOffPolicy)
        val retryPolicy = SimpleRetryPolicy()
        retryPolicy.maxAttempts = 30
        retryTemplate.setRetryPolicy(retryPolicy)
        retryTemplate.setListeners(arrayOf(LogRetryListener()))
        return retryTemplate
    }

    @Bean
    fun objectMapper(): ObjectMapper {
        return jacksonObjectMapper()
            .findAndRegisterModules()
            .registerModule(JavaTimeModule())
            .setDateFormat(StdDateFormat())
    }

    @Bean
    fun jackson2JsonEncoder(): Jackson2JsonEncoder {
        return Jackson2JsonEncoder(objectMapper())
    }

    @Bean
    fun jackson2JsonDecoder(): Jackson2JsonDecoder {
        return Jackson2JsonDecoder(objectMapper())
    }

    @Bean
    fun webFluxConfigurer(encoder: Jackson2JsonEncoder, decoder: Jackson2JsonDecoder): WebFluxConfigurer {
        return object : WebFluxConfigurer {
            override fun configureHttpMessageCodecs(configurer: ServerCodecConfigurer) {
                configurer.defaultCodecs().jackson2JsonEncoder(encoder)
                configurer.defaultCodecs().jackson2JsonDecoder(decoder)
            }
            override fun configurePathMatching(configurer: PathMatchConfigurer) {
                configurer.setUseTrailingSlashMatch(true)
            }
        }
    }
}
