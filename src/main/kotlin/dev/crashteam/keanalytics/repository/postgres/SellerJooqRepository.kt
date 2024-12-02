package dev.crashteam.keanalytics.repository.postgres

import dev.crashteam.keanalytics.db.model.tables.Sellers.*
import dev.crashteam.keanalytics.db.model.tables.pojos.Sellers
import org.jooq.DSLContext
import org.springframework.stereotype.Repository

@Repository
class SellerJooqRepository(
    private val dsl: DSLContext
) : SellerRepository {

    override fun save(seller: Sellers) {
        val s = SELLERS
        dsl.insertInto(s, s.SELLER_ID, s.ACCOUNT_ID, s.TITLE, s.LINK)
            .values(seller.sellerId, seller.accountId, seller.title, seller.link)
            .onConflict(s.SELLER_ID, s.ACCOUNT_ID)
            .doUpdate().set(
                mapOf(
                    s.TITLE to seller.title,
                    s.LINK to seller.link,
                )
            ).execute()
    }

    override fun saveBatch(sellers: Collection<Sellers>): IntArray {
        val s = SELLERS
        return dsl.batch(
            sellers.map { seller ->
                dsl.insertInto(
                    s, s.SELLER_ID, s.ACCOUNT_ID, s.TITLE, s.LINK
                ).values(
                    seller.sellerId, seller.accountId, seller.title, seller.link
                ).onConflict().doUpdate()
                    .set(s.SELLER_ID, seller.sellerId)
                    .set(s.ACCOUNT_ID, seller.accountId)
                    .set(s.LINK, seller.link)
                    .set(s.TITLE, seller.title)
            },
        ).execute()
    }

    override fun findBySellerLink(sellerLink: String): Sellers? {
        val s = SELLERS
        return dsl.selectFrom(s)
            .where(s.LINK.eq(sellerLink))
            .fetchOneInto(Sellers::class.java)
    }

    override fun findAccountIdsBySellerLink(sellerLink: String): List<Long> {
        val s = SELLERS
        return dsl.select(s.ACCOUNT_ID)
            .from(s)
            .where(s.LINK.eq(sellerLink))
            .fetch(s.ACCOUNT_ID)
    }

    override fun findByAccountId(accountId: Long): List<Sellers> {
        val s = SELLERS
        return dsl.selectFrom(s)
            .where(s.ACCOUNT_ID.eq(accountId))
            .fetchInto(Sellers::class.java)
    }
}
