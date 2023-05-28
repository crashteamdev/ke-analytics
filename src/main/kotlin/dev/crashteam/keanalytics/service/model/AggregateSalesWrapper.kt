package dev.crashteam.keanalytics.service.model

data class AggregateSalesWrapper(
    val data: List<AggregateSalesProduct>,
    val meta: AggregateSalesMetadata,
)

data class AggregateSalesMetadata(
    val total: Long,
    val page: Int,
    val pages: Int,
    val pageSize: Int,
)
