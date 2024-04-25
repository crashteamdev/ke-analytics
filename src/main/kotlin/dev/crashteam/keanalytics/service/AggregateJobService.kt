package dev.crashteam.keanalytics.service

import dev.crashteam.keanalytics.service.model.StatType
import org.springframework.data.redis.core.RedisTemplate
import org.springframework.stereotype.Service
import java.time.Duration
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.ZoneOffset
import java.util.concurrent.TimeUnit

@Service
class AggregateJobService(
    private val reactiveRedisTemplate: RedisTemplate<String, Long>
) {

    fun checkCategoryAlreadyAggregated(tableName: String, categoryId: Long, statType: StatType): Boolean {
        val key = toKey(tableName, categoryId, statType)
        val keyValue = reactiveRedisTemplate.opsForValue().get(key)
        return keyValue != null && keyValue >= 1
    }

    fun putCategoryAggregate(tableName: String, categoryId: Long, statType: StatType) {
        val key = toKey(tableName, categoryId, statType)
        reactiveRedisTemplate.opsForValue().set(key, 1)

        val now = LocalDateTime.now().atOffset(ZoneOffset.UTC)
        val midnight = now.plusDays(1).with(LocalTime.MIDNIGHT)
        val ttlSeconds = Duration.between(now, midnight).seconds
        reactiveRedisTemplate.expire(key, ttlSeconds, TimeUnit.SECONDS)
    }

    private fun toKey(tableName: String, categoryId: Long, statType: StatType) =
        "ke-$tableName-$categoryId-${statType.name.lowercase()}"

}
