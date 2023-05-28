package dev.crashteam.keanalytics.domain.mongo

import org.bson.types.ObjectId
import org.springframework.data.mongodb.core.index.Indexed
import org.springframework.data.mongodb.core.mapping.Document
import org.springframework.data.mongodb.core.mapping.MongoId

@Document("sellers")
data class SellerDetailDocument(
    @Indexed(unique = true)
    val sellerId: Long,
    @Indexed
    val accountId: Long,
    val title: String,
    @Indexed(unique = true)
    val link: String,

    @MongoId
    val id: ObjectId = ObjectId(),
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as SellerDetailDocument

        if (sellerId != other.sellerId) return false
        if (accountId != other.accountId) return false

        return true
    }

    override fun hashCode(): Int {
        var result = sellerId.hashCode()
        result = 31 * result + accountId.hashCode()
        return result
    }
}
