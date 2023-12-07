package dev.crashteam.keanalytics.stream.listener.redis

import org.springframework.data.redis.connection.stream.Record

interface BatchStreamListener<S, V : Record<S, *>> {
    suspend fun onMessage(messages: List<V>)
}
