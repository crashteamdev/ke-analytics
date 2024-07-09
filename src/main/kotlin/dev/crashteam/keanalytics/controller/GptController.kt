package dev.crashteam.keanalytics.controller

import dev.crashteam.keanalytics.repository.postgres.UserRepository
import dev.crashteam.keanalytics.service.ProductServiceAnalytics
import dev.crashteam.keanalytics.service.UserRestrictionService
import dev.crashteam.openapi.gpt.analytics.api.GptApi
import dev.crashteam.openapi.gpt.analytics.model.ProductSkuHistory
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.server.ServerWebExchange
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toFlux
import reactor.kotlin.core.publisher.toMono
import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.time.temporal.ChronoUnit

@RestController
@RequestMapping(path = ["v1"], produces = [MediaType.APPLICATION_JSON_VALUE])
class GptController(
    private val userRepository: UserRepository,
    private val productServiceAnalytics: ProductServiceAnalytics,
    private val userRestrictionService: UserRestrictionService,
) : GptApi {

    override fun gptProductSkuHistory(
        X_API_KEY: String,
        productId: Long,
        skuId: Long,
        fromTime: OffsetDateTime,
        toTime: OffsetDateTime,
        exchange: ServerWebExchange
    ): Mono<ResponseEntity<Flux<ProductSkuHistory>>> {
        return checkRequestDaysPermission(
            X_API_KEY,
            fromTime.toLocalDateTime(),
            toTime.toLocalDateTime()
        ).flatMap { access ->
            if (access == false) {
                return@flatMap ResponseEntity.status(HttpStatus.FORBIDDEN).build<Flux<ProductSkuHistory>>().toMono()
            }
            val productAnalytics = productServiceAnalytics.getProductAnalytics(
                productId,
                skuId,
                fromTime.toLocalDateTime(),
                toTime.toLocalDateTime()
            )
            if (productAnalytics.isEmpty()) {
                return@flatMap ResponseEntity.notFound().build<Flux<ProductSkuHistory>>().toMono()
            }
            val productSkuHistoryList = productAnalytics.map {
                ProductSkuHistory().apply {
                    this.productId = productId
                    this.skuId = skuId
                    this.name = it.title
                    this.orderAmount = it.orderAmount
                    this.reviewsAmount = it.reviewAmount
                    this.totalAvailableAmount = it.totalAvailableAmount
                    this.fullPrice = it.fullPrice?.toDouble()
                    this.purchasePrice = it.purchasePrice.toDouble()
                    this.availableAmount = it.availableAmount
                    this.salesAmount = it.salesAmount.toDouble()
                    this.photoKey = it.photoKey
                    this.date = it.date
                }
            }
            ResponseEntity.ok(productSkuHistoryList.toFlux()).toMono()
        }
    }

    private fun checkRequestDaysPermission(
        apiKey: String,
        fromTime: LocalDateTime,
        toTime: LocalDateTime
    ): Mono<Boolean> {
        return true.toMono()
    }
}
