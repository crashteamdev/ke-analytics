package dev.crashteam.keanalytics.client.kazanexpress.model

data class KazanExpressGQLQuery<T>(
    val operationName: String,
    val query: String,
    val variables: T
)

data class ShopGQLQueryVariables(
    val queryInput: ShopGQLQueryInput
)

data class ShopGQLQueryInput(
    val shopId: String,
    val showAdultContent: String,
    val filters: List<Any> = emptyList(),
    val sort: String,
    val pagination: ShopGQLPagination
)

data class ShopGQLPagination(
    val offset: Long,
    val limit: Long
)
