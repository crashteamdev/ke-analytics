package dev.crashteam.keanalytics.stream.model

data class KeProductItemStreamRecord(
    val productId: Long,
    val time: Long,
    val title: String,
    val totalAvailableAmount: Long,
    val orders: Long,
    val description: String,
    val attributes: List<String>,
    val tags: List<String>,
    val reviewsAmount: Long,
    val rating: String,
    val skuList: List<KeItemSkuStreamRecord>,
    val seller: KeProductSellerStreamRecord,
    val category: KeProductCategoryStreamRecord,
    val isEco: Boolean,
    val isAdult: Boolean,
    val characteristics: List<KeProductCharacteristicsStreamRecord>
)

data class KeProductCharacteristicsStreamRecord(
    val id: Long,
    val title: String,
    val values: List<KeProductCharacteristicStreamRecord>
)

data class KeProductCharacteristicStreamRecord(
    val id: Long,
    val title: String,
    val value: String
)

data class KeProductCategoryStreamRecord(
    val id: Long,
    val title: String,
    val productAmount: Long,
    val parent: KeProductCategoryStreamRecord?
)

data class KeItemSkuStreamRecord(
    val skuId: Long,
    val photoKey: String,
    val characteristics: List<KeItemCharacteristicStreamRecord>,
    val purchasePrice: String,
    val fullPrice: String?,
    val availableAmount: Long,
)

data class KeItemCharacteristicStreamRecord(
    val type: String,
    val title: String,
    val value: String,
)

data class KeProductSellerStreamRecord(
    val id: Long,
    val accountId: Long,
    val sellerLink: String,
    val sellerTitle: String,
    val reviews: Long,
    val orders: Long,
    val rating: String,
    val registrationDate: Long,
    val description: String?,
    val contacts: List<KeProductSellerContact>,
)

data class KeProductSellerContact(
    val type: String,
    val value: String,
)
