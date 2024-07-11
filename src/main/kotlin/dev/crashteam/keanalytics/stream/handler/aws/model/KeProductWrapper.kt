package dev.crashteam.keanalytics.stream.handler.aws.model

import com.google.protobuf.Timestamp
import dev.crashteam.ke.scrapper.data.v1.KeProductChange

data class KeProductWrapper(
    val product: KeProductChange,
    val eventTime: Timestamp,
)
