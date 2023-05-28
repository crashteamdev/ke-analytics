package dev.crashteam.keanalytics.client.kazanexpress.model

data class CategoryGQLQuery(
    val operationName: String,
    val query: String,
    val variables: CategoryGQLQueryVariables
)

data class CategoryGQLQueryVariables(
    val queryInput: CategoryGQLQueryInput
)

data class CategoryGQLQueryInput(
    val categoryId: String,
    val pagination: CategoryGQLQueryInputPagination,
    val showAdultContent: String,
    val filters: List<Any> = emptyList(),
    val sort: String,
)

data class CategoryGQLQueryInputPagination(
    val offset: Long,
    val limit: Int
)
