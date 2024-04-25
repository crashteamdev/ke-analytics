package dev.crashteam.keanalytics.service

import dev.crashteam.keanalytics.service.model.StatType
import kotlinx.coroutines.reactor.awaitSingleOrNull
import kotlinx.coroutines.runBlocking
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.stereotype.Service
import java.time.Duration
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.ZoneOffset

@Service
class AggregateJobService(
    private val reactiveRedisTemplate: ReactiveRedisTemplate<String, Long>
) {

    fun checkCategoryAlreadyAggregated(
        tableName: String,
        categoryId: Long,
        statType: StatType
    ): Boolean = runBlocking {
        val key = toKey(tableName, categoryId, statType)
        val keyValue = reactiveRedisTemplate.opsForValue().get(key).awaitSingleOrNull()
        keyValue != null && keyValue >= 1
    }

    fun putCategoryAggregate(tableName: String, categoryId: Long, statType: StatType) = runBlocking {
        val key = toKey(tableName, categoryId, statType)
        reactiveRedisTemplate.opsForValue().set(key, 1).awaitSingleOrNull()

        val now = LocalDateTime.now().atOffset(ZoneOffset.UTC)
        val midnight = now.plusDays(1).with(LocalTime.MIDNIGHT)
        val ttlSeconds = Duration.between(now, midnight).seconds
        reactiveRedisTemplate.expire(key, Duration.ofSeconds(ttlSeconds)).awaitSingleOrNull()
    }

    private fun toKey(tableName: String, categoryId: Long, statType: StatType) =
        "ke-$tableName-$categoryId-${statType.name.lowercase()}"

}
