package dev.crashteam.keanalytics.client.kazanexpress.model.proxy

data class StyxResponse<T>(
    val code: Int,
    val originalStatus: Int,
    val message: String? = null,
    val url: String,
    val body: T? = null,
)
