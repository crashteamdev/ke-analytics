package dev.crashteam.keanalytics.config

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.dynamodbv2.model.BillingMode
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration.*
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.SimpleRecordsFetcherFactory
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker
import com.amazonaws.services.kinesis.metrics.impl.NullMetricsFactory
import dev.crashteam.keanalytics.config.properties.AwsStreamProperties
import dev.crashteam.keanalytics.stream.listener.aws.payment.KePaymentEventStreamProcessorFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Scope
import java.time.Duration
import java.util.*

@Configuration
class AwsSteamConfig(
    private val awsStreamProperties: AwsStreamProperties,
    private val kePaymentEventStreamProcessorFactory: KePaymentEventStreamProcessorFactory,
) {

    @Value("\${spring.application.name}")
    private lateinit var appName: String

    @Bean
    @Scope(value = "prototype")
    fun paymentStreamWorker(): Worker {
        val awsCredentials = BasicAWSCredentials(awsStreamProperties.accessKey, awsStreamProperties.secretKey)
        val consumerConfig = KinesisClientLibConfiguration(
            "$appName-payment",
            awsStreamProperties.paymentStream.name,
            awsStreamProperties.kinesisEndpoint,
            awsStreamProperties.dinamoDbEndpoint,
            InitialPositionInStream.LATEST,
            AWSStaticCredentialsProvider(awsCredentials),
            AWSStaticCredentialsProvider(awsCredentials),
            AWSStaticCredentialsProvider(awsCredentials),
            awsStreamProperties.paymentStream.failOverTimeMillis,
            "${awsStreamProperties.paymentStream.consumerName}-${UUID.randomUUID()}",
            awsStreamProperties.paymentStream.maxRecords,
            DEFAULT_IDLETIME_BETWEEN_READS_MILLIS,
            DEFAULT_DONT_CALL_PROCESS_RECORDS_FOR_EMPTY_RECORD_LIST,
            DEFAULT_PARENT_SHARD_POLL_INTERVAL_MILLIS,
            DEFAULT_SHARD_SYNC_INTERVAL_MILLIS,
            DEFAULT_CLEANUP_LEASES_UPON_SHARDS_COMPLETION,
            ClientConfiguration(),
            ClientConfiguration(),
            ClientConfiguration(),
            DEFAULT_TASK_BACKOFF_TIME_MILLIS,
            DEFAULT_METRICS_BUFFER_TIME_MILLIS,
            DEFAULT_METRICS_MAX_QUEUE_SIZE,
            DEFAULT_VALIDATE_SEQUENCE_NUMBER_BEFORE_CHECKPOINTING,
            awsStreamProperties.region,
            DEFAULT_SHUTDOWN_GRACE_MILLIS,
            BillingMode.PAY_PER_REQUEST,
            SimpleRecordsFetcherFactory(),
            Duration.ofMinutes(1).toMillis(),
            Duration.ofMinutes(5).toMillis(),
            Duration.ofMinutes(30).toMillis(),
        )
        consumerConfig.withTimeoutInSeconds(awsStreamProperties.paymentStream.timeoutInSec)

        return Worker.Builder()
            .recordProcessorFactory(kePaymentEventStreamProcessorFactory)
            .config(consumerConfig)
            .metricsFactory(NullMetricsFactory())
            .build()
    }

}
