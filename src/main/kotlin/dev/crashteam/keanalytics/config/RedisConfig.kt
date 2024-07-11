package dev.crashteam.keanalytics.config

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.google.protobuf.Message
import dev.crashteam.keanalytics.client.kazanexpress.model.ProductResponse
import dev.crashteam.keanalytics.config.properties.RedisProperties
import dev.crashteam.keanalytics.repository.clickhouse.model.ChCategoryOverallInfo
import dev.crashteam.keanalytics.repository.redis.ApiKeyUserSessionInfo
import dev.crashteam.keanalytics.service.model.CategoryAnalyticsCacheableWrapper
import dev.crashteam.keanalytics.service.model.SellerOverallInfo
import dev.crashteam.mp.external.analytics.category.GetCategoryAnalyticsResponse
import mu.KotlinLogging
import org.springframework.boot.autoconfigure.cache.RedisCacheManagerBuilderCustomizer
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.autoconfigure.data.redis.LettuceClientConfigurationBuilderCustomizer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.redis.RedisSystemException
import org.springframework.data.redis.cache.RedisCacheConfiguration
import org.springframework.data.redis.cache.RedisCacheManager.RedisCacheManagerBuilder
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration
import org.springframework.data.redis.connection.stream.ObjectRecord
import org.springframework.data.redis.connection.stream.ReadOffset
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.data.redis.serializer.*
import org.springframework.data.redis.serializer.RedisSerializationContext.SerializationPair.fromSerializer
import org.springframework.data.redis.stream.StreamReceiver
import java.nio.ByteBuffer
import java.time.Duration

private val log = KotlinLogging.logger {}

@Configuration
class RedisConfig(
    private val objectMapper: ObjectMapper,
    private val redisProperties: RedisProperties,
) {

    @Bean
    fun reactiveRedisTemplate(
        redisConnectionFactory: ReactiveRedisConnectionFactory
    ): ReactiveRedisTemplate<String, Long> {
        val jdkSerializationRedisSerializer = JdkSerializationRedisSerializer()
        val stringRedisSerializer = StringRedisSerializer.UTF_8
        val longToStringSerializer = GenericToStringSerializer(Long::class.java)
        return ReactiveRedisTemplate(
            redisConnectionFactory,
            RedisSerializationContext.newSerializationContext<String, Long>(jdkSerializationRedisSerializer)
                .key(stringRedisSerializer).value(longToStringSerializer).build()
        )
    }

    @Bean
    fun messageReactiveRedisTemplate(
        redisConnectionFactory: ReactiveRedisConnectionFactory
    ): ReactiveRedisTemplate<String, String> {
        val serializationContext: RedisSerializationContext<String, String> = RedisSerializationContext
            .newSerializationContext<String, String>(StringRedisSerializer())
            .key(StringRedisSerializer())
            .value(GenericToStringSerializer(String::class.java))
            .build()
        return ReactiveRedisTemplate(redisConnectionFactory, serializationContext);
    }

    @Bean
    fun apiKeySessionRedisTemplate(
        redisConnectionFactory: ReactiveRedisConnectionFactory
    ): ReactiveRedisTemplate<String, ApiKeyUserSessionInfo> {
        val jdkSerializationRedisSerializer = JdkSerializationRedisSerializer()
        val stringRedisSerializer = StringRedisSerializer.UTF_8
        val jackson2JsonRedisSerializer = Jackson2JsonRedisSerializer(ApiKeyUserSessionInfo::class.java)
        return ReactiveRedisTemplate(
            redisConnectionFactory,
            RedisSerializationContext.newSerializationContext<String, ApiKeyUserSessionInfo>(
                jdkSerializationRedisSerializer
            ).key(stringRedisSerializer).value(jackson2JsonRedisSerializer).build()
        )
    }

    @Bean
    fun redisCacheManagerBuilderCustomizer(): RedisCacheManagerBuilderCustomizer {
        return RedisCacheManagerBuilderCustomizer { builder: RedisCacheManagerBuilder ->
            val configurationMap: MutableMap<String, RedisCacheConfiguration> = HashMap()
            configurationMap[KE_CLIENT_CACHE_NAME] = RedisCacheConfiguration.defaultCacheConfig()
                .serializeValuesWith(fromSerializer(object : RedisSerializer<Any> {
                    override fun serialize(t: Any?): ByteArray {
                        return objectMapper.writeValueAsBytes(t)
                    }

                    override fun deserialize(bytes: ByteArray?): Any? {
                        return if (bytes != null) {
                            objectMapper.readValue<ProductResponse>(bytes)
                        } else null
                    }

                })).entryTtl(Duration.ofSeconds(120))
            configurationMap[CATEGORY_OVERALL_INFO_CACHE] = RedisCacheConfiguration.defaultCacheConfig()
                .serializeValuesWith(redisJsonSerializer(ChCategoryOverallInfo::class.java))
                .entryTtl(Duration.ofSeconds(21600))
            configurationMap[SELLER_OVERALL_INFO_CACHE_NAME] = RedisCacheConfiguration.defaultCacheConfig()
                .serializeValuesWith(redisJsonSerializer(SellerOverallInfo::class.java))
                .entryTtl(Duration.ofSeconds(21600))
            configurationMap[EXTERNAL_CATEGORY_ANALYTICS_CACHE_NAME] = RedisCacheConfiguration.defaultCacheConfig()
                .serializeValuesWith(redisJsonSerializer(CategoryAnalyticsCacheableWrapper::class.java))
                .entryTtl(Duration.ofHours(3))
            builder.withInitialCacheConfigurations(configurationMap)
        }
    }

    @Bean
    @ConditionalOnProperty(prefix = "spring.redis", value = ["ssl"], havingValue = "true")
    fun builderCustomizer(): LettuceClientConfigurationBuilderCustomizer {
        return LettuceClientConfigurationBuilderCustomizer { builder: LettuceClientConfiguration.LettuceClientConfigurationBuilder ->
            builder.useSsl().disablePeerVerification()
        }
    }

    private inline fun <reified T> redisJsonSerializer(
        valueClass: Class<T>
    ): RedisSerializationContext.SerializationPair<Any> {
        val objectMapper = jacksonObjectMapper().registerModules(JavaTimeModule())
        return fromSerializer(object : RedisSerializer<Any> {
            override fun serialize(t: Any?): ByteArray {
                return objectMapper.writeValueAsBytes(t)
            }

            override fun deserialize(bytes: ByteArray?): Any? {
                return if (bytes != null) {
                    objectMapper.readValue(bytes, valueClass)
                } else null
            }

        })
    }

    companion object {
        const val KE_CLIENT_CACHE_NAME = "ke-products-info"
        const val CATEGORY_OVERALL_INFO_CACHE = "ke-category-overall-info"
        const val SELLER_OVERALL_INFO_CACHE_NAME = "ke-seller-overall-info"
        const val EXTERNAL_CATEGORY_ANALYTICS_CACHE_NAME = "mm-external-category-analytics"
    }
}
