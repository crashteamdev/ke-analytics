package dev.crashteam.keanalytics.client.kazanexpress.model.proxy

data class ProxyRequestBody(
    val url: String,
    val httpMethod: String,
    val context: List<ProxyRequestContext>? = null,
)

data class ProxyRequestContext(
    val key: String,
    val value: Any
)
