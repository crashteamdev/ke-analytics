package dev.crashteam.keanalytics.service

import dev.crashteam.keanalytics.db.model.tables.pojos.Sellers
import dev.crashteam.keanalytics.repository.postgres.SellerRepository
import org.springframework.stereotype.Service

@Service
class SellerService(
    private val sellerRepository: SellerRepository
) {

    fun findSellersByLink(sellerLink: String): List<Sellers> {
        val seller = sellerRepository.findBySellerLink(sellerLink) ?: return emptyList()
        return sellerRepository.findByAccountId(seller.accountId)
    }
}

