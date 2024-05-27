package dev.crashteam.keanalytics.service.model

enum class StatType(val days: Int) {
    WEEK(7),
    TWO_WEEK(14),
    MONTH(30),
    TWO_MONTH(60),
}
