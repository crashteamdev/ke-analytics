package dev.crashteam.keanalytics.repository.mongo

import com.mongodb.BasicDBObject
import com.mongodb.bulk.BulkWriteResult
import com.mongodb.client.model.Filters
import com.mongodb.client.model.UpdateOneModel
import com.mongodb.client.model.UpdateOptions
import dev.crashteam.keanalytics.domain.mongo.SellerDetailDocument
import org.bson.Document
import org.springframework.data.mongodb.core.ReactiveMongoTemplate
import reactor.core.publisher.Mono

class SellerCustomRepositoryImpl(
    private val reactiveMongoTemplate: ReactiveMongoTemplate,
) : SellerCustomRepository {

    override fun saveSellerBatch(sellers: Collection<SellerDetailDocument>): Mono<BulkWriteResult> {
        val updates = sellers.map { seller ->
            val sellerDocument = Document()
            reactiveMongoTemplate.converter.write(seller, sellerDocument)
            UpdateOneModel<Document>(
                Filters.eq("sellerId", seller.sellerId),
                BasicDBObject(
                    "\$set",
                    Document("sellerId", seller.sellerId)
                        .append("accountId", seller.accountId)
                        .append("title", seller.title)
                        .append("link", seller.link)
                ),
                UpdateOptions().upsert(true)
            )
        }
        val collectionName = reactiveMongoTemplate.getCollectionName(SellerDetailDocument::class.java)

        return reactiveMongoTemplate.getCollection(collectionName).flatMap {
            Mono.from(it.bulkWrite(updates))
        }
    }
}
