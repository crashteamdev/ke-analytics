package dev.crashteam.keanalytics.repository.clickhouse.model

import java.time.LocalDate

data class ChProductPositionHistory(
    val date: LocalDate,
    val position: Long,
)
