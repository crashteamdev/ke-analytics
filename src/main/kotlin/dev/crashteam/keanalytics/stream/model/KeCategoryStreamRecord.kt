package dev.crashteam.keanalytics.stream.model

data class KeCategoryStreamRecord(
    val id: Long,
    val adult: Boolean,
    val eco: Boolean,
    val title: String,
    val children: List<KeCategoryStreamRecord>?,
    val time: Long,
)
