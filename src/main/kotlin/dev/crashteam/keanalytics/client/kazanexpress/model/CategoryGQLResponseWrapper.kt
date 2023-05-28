package dev.crashteam.keanalytics.client.kazanexpress.model

data class CategoryGQLSearchResponse(
    val category: CategoryGQLInfo,
    val categoryTree: List<CategoryGQLTreeDataWrapper>,
    val items: List<CategoryGQLCatalogCard>?,
    val total: Long
)

data class CategoryGQLInfo(
    val id: Long,
    val title: String,
    val parent: CategoryGQLInfo? = null
)

data class CategoryGQLTreeDataWrapper(
    val category: CategoryGQLTreeData
)

data class CategoryGQLTreeData(
    val id: Long,
    val title: String,
    val parent: CategoryGQLTreeDataParentId? = null
)

data class CategoryGQLTreeDataParentId(
    val id: Long
)

data class CategoryGQLCatalogCard(
    val catalogCard: SkuGroupCard
)

data class SkuGroupCard(
    val id: Long,
    val productId: Long,
    val title: String,
    val adult: Boolean,
    val characteristicValues: List<CategoryGQLCharacteristicValue>?,
    val feedbackQuantity: Long,
    val minFullPrice: Long,
    val minSellPrice: Long,
    val ordersQuantity: Long
)

data class CategoryGQLCharacteristicValue(
    val id: Long,
    val title: String,
    val value: String
)
