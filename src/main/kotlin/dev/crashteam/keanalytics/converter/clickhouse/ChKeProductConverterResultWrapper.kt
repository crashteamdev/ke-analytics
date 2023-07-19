package dev.crashteam.keanalytics.converter.clickhouse

import dev.crashteam.keanalytics.repository.clickhouse.model.ChKeProduct

data class ChKeProductConverterResultWrapper(
    val result: List<ChKeProduct>
)
