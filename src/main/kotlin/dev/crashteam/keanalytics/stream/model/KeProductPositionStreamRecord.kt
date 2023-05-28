package dev.crashteam.keanalytics.stream.model

data class KeProductPositionStreamRecord(
    val position: Long,
    val productId: Long,
    val skuId: Long,
    val categoryId: Long,
    val time: Long
)
