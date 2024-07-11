package dev.crashteam.keanalytics.config.properties

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "aws-stream")
data class AwsStreamProperties(
    val kinesisEndpoint: String,
    val dinamoDbEndpoint: String,
    val accessKey: String,
    val secretKey: String,
    val region: String,
    val keStream: StreamProperties,
    val paymentStream: StreamProperties,
)

data class StreamProperties(
    val name: String,
    val timeoutInSec: Int,
    val maxRecords: Int,
    val failOverTimeMillis: Long,
    val consumerName: String,
)
