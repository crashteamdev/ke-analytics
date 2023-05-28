package dev.crashteam.keanalytics.client.kazanexpress.model

data class KazanExpressGQLQueryResponse<T>(
    val data: KazanExpressGQLQueryResponseData<T>?,
    val errors: List<GQLError>?
)

data class KazanExpressGQLQueryResponseData<T>(
    val makeSearch: T
)

// Shop query
data class ShopGQLQueryResponse(
    val items: List<ShopGQLCatalogCard>
)

data class ShopGQLCatalogCard(
    val catalogCard: ShopSkuGroupCard
)

data class ShopSkuGroupCard(
    val id: Long,
    val productId: Long,
    val title: String,
    val adult: Boolean,
    val characteristicValues: List<SkuGQLCharacteristicValue>?,
    val feedbackQuantity: Long,
    val minFullPrice: Long,
    val minSellPrice: Long,
    val ordersQuantity: Long
)

data class SkuGQLCharacteristicValue(
    val id: Long,
    val title: String,
    val value: String
)

data class GQLError(
    val message: String?
)
