package dev.crashteam.keanalytics.report.model

import java.io.InputStream

data class Report(
    val name: String,
    val stream: InputStream
)
