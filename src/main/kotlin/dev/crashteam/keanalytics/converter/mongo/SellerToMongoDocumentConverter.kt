package dev.crashteam.keanalytics.converter.mongo

import dev.crashteam.keanalytics.client.kazanexpress.model.*
import dev.crashteam.keanalytics.converter.DataConverter
import dev.crashteam.keanalytics.domain.mongo.ProductContactDocument
import dev.crashteam.keanalytics.domain.mongo.SellerDocument
import org.springframework.stereotype.Component

@Component
class SellerToMongoDocumentConverter : DataConverter<Seller, SellerDocument> {

    override fun convert(source: Seller): SellerDocument {
        return SellerDocument(
            id = source.id,
            title = source.title,
            link = source.link,
            description = source.description,
            rating = source.rating,
            sellerAccountId = source.sellerAccountId,
            isEco = source.isEco,
            adultCategory = source.adultCategory,
            contacts = source.contacts.map { ProductContactDocument(it.type, it.value) }
        )
    }

}
