package dev.crashteam.keanalytics.client.kazanexpress

class KazanExpressClientException(status: Int, rawResponseBody: String, message: String) : RuntimeException(message)
