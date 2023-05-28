package dev.crashteam.keanalytics.service

import dev.crashteam.keanalytics.domain.mongo.SellerDetailDocument
import dev.crashteam.keanalytics.repository.mongo.SellerRepository
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux

@Service
class SellerService(
    private val sellerRepository: SellerRepository
) {

    fun findSellersByLink(sellerLink: String): Flux<SellerDetailDocument> {
        return sellerRepository.findByLink(sellerLink).flatMapMany { sellerDetailDocument ->
            sellerRepository.findByAccountId(sellerDetailDocument.accountId)
        }
    }
}
