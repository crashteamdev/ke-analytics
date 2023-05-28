package dev.crashteam.keanalytics.repository.mongo

import com.mongodb.bulk.BulkWriteResult
import com.mongodb.client.result.UpdateResult
import dev.crashteam.keanalytics.domain.mongo.ProductDocument
import dev.crashteam.keanalytics.repository.mongo.model.*
import dev.crashteam.keanalytics.repository.mongo.pageable.PageResult
import dev.crashteam.keanalytics.repository.mongo.pageable.ProductHistoryPageResult
import org.reactivestreams.Publisher
import org.springframework.data.domain.Pageable
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.LocalDateTime
import java.time.OffsetDateTime

interface ProductCustomRepository {

    fun saveProduct(product: ProductDocument): Mono<UpdateResult>

    suspend fun saveProductBatch(products: Collection<ProductDocument>): Publisher<BulkWriteResult>

    fun findProduct(productId: Long): Mono<ProductDocument>

    fun findShopProducts(shopName: String): Flux<ProductDocument>

    fun findTopShopsByTotalOrders(): Flux<ShopTotalOrder>

    fun findTopProductByOrders(limit: Long): Flux<ProductTotalOrderAggregate>

    fun findProductByProperties(
        filter: FindProductFilter,
        sort: Array<String>,
        page: Pageable,
    ): Mono<PageResult<ProductDocument>>

    suspend fun getProductOrders(
        productId: Long,
        fromTime: LocalDateTime,
        toTime: LocalDateTime,
    ): Mono<ProductTotalOrdersAggregate>

    suspend fun findProductHistoryByProductId(
        productId: LongArray,
        fromTime: LocalDateTime,
        toTime: LocalDateTime,
    ): Flux<MultipleProductHistorySales>

    @Deprecated("Deprecated case of new schema method")
    suspend fun findProductHistoryBySkuId(
        productId: Long,
        skuId: Long,
        fromTime: LocalDateTime,
        toTime: LocalDateTime,
        page: Pageable,
    ): ProductHistoryPageResult

    suspend fun findProductHistoryByCategory(
        category: String,
        fromTime: LocalDateTime,
        toTime: LocalDateTime,
        sort: Array<String>,
        page: Pageable,
    ): Flux<ProductCategoryAggregate>

    suspend fun findProductHistoryByCategoryWithLimit(
        category: String,
        fromTime: LocalDateTime,
        toTime: LocalDateTime,
        limit: Int
    ): Mono<MutableList<ProductHistorySkuAggregate>>

    suspend fun findProductHistoryBySellerPageable(
        title: String?,
        link: String?,
        fromTime: LocalDateTime,
        toTime: LocalDateTime,
        sort: Array<String>,
        page: Pageable,
    ): Flux<ProductSellerAggregate>


    suspend fun findProductHistoryBySeller(
        title: String?,
        link: String?,
        fromTime: LocalDateTime,
        toTime: LocalDateTime,
    ): Flux<ProductHistorySkuAggregate>

    suspend fun findDistinctSellerIds(): Flux<Long>

    suspend fun findProductIdsByCategory(category: String): Flux<Long>

    suspend fun findSellerIdsByCategory(category: String): Flux<Long>

    suspend fun findProductIdsBySeller(sellerLink: String): Flux<Long>
}
