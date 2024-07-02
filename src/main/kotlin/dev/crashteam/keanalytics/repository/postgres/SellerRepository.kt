package dev.crashteam.keanalytics.repository.postgres

import dev.crashteam.keanalytics.db.model.tables.pojos.Sellers

interface SellerRepository {
    fun save(seller: Sellers)

    fun saveBatch(sellers: Collection<Sellers>): IntArray

    fun findBySellerLink(sellerLink: String): Sellers?

    fun findByAccountId(accountId: Long): List<Sellers>
}
