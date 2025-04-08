package dev.crashteam.keanalytics.controller

import dev.crashteam.keanalytics.db.model.tables.pojos.Reports
import dev.crashteam.keanalytics.report.ReportFileService
import dev.crashteam.keanalytics.report.ReportService
import dev.crashteam.keanalytics.repository.postgres.ReportRepository
import dev.crashteam.keanalytics.repository.postgres.UserRepository
import dev.crashteam.keanalytics.service.*
import dev.crashteam.keanalytics.service.exception.UserSubscriptionGiveawayException
import dev.crashteam.openapi.keanalytics.api.*
import dev.crashteam.openapi.keanalytics.model.*
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import mu.KotlinLogging
import org.springframework.core.io.InputStreamResource
import org.springframework.core.io.Resource
import org.springframework.http.HttpHeaders
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
import java.math.RoundingMode
import java.security.Principal
import java.time.*
import java.time.temporal.ChronoUnit
import java.util.*

private val log = KotlinLogging.logger {}

@RestController
@RequestMapping(path = ["v2"], produces = [MediaType.APPLICATION_JSON_VALUE])
class MarketDbApiV2Controller(
    private val sellerService: SellerService,
    private val userRepository: UserRepository,
    private val userRestrictionService: UserRestrictionService,
    private val reportFileService: ReportFileService,
    private val reportService: ReportService,
    private val reportRepository: ReportRepository,
    private val productServiceAnalytics: ProductServiceAnalytics,
    private val userSubscriptionService: UserSubscriptionService,
    private val productService: ProductServiceV2,
) : CategoryApi, ProductApi, SellerApi, ReportApi, ReportsApi, PromoCodeApi, SubscriptionApi {

    override fun productOverallInfo(
        xRequestID: UUID,
        X_API_KEY: String,
        productId: Long,
        skuId: Long,
        fromTime: OffsetDateTime,
        toTime: OffsetDateTime,
        exchange: ServerWebExchange
    ): Mono<ResponseEntity<ProductOverallInfo200Response>> {
        return checkRequestDaysPermission(
            X_API_KEY,
            fromTime.toLocalDateTime(),
            toTime.toLocalDateTime()
        ).flatMap { access ->
            val fromTimeLocalDateTime = fromTime.toLocalDateTime()
            val toTimeLocalDateTime = toTime.toLocalDateTime()
            val productAdditionalInfo = productServiceAnalytics.getProductAdditionalInfo(
                productId.toString(),
                skuId.toString(),
                fromTimeLocalDateTime,
                toTimeLocalDateTime
            ) ?: return@flatMap ResponseEntity.notFound().build<ProductOverallInfo200Response>().toMono()

            return@flatMap ResponseEntity.ok(ProductOverallInfo200Response().apply {
                firstDiscovered = productAdditionalInfo.firstDiscovered.atOffset(ZoneOffset.UTC)
            }).toMono()
        }
    }

    override fun productSkuHistory(
        xRequestID: UUID,
        X_API_KEY: String,
        productId: Long,
        skuId: Long,
        fromTime: OffsetDateTime,
        toTime: OffsetDateTime,
        limit: Int,
        offset: Int,
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

    override fun getProductSales(
        xRequestID: UUID,
        X_API_KEY: String,
        productIds: MutableList<Long>,
        fromTime: OffsetDateTime,
        toTime: OffsetDateTime,
        exchange: ServerWebExchange
    ): Mono<ResponseEntity<Flux<GetProductSales200ResponseInner>>> {
        return checkRequestDaysPermission(
            X_API_KEY,
            fromTime.toLocalDateTime(),
            toTime.toLocalDateTime()
        ).flatMap { access ->
            if (access == false) {
                return@flatMap ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .build<Flux<GetProductSales200ResponseInner>>().toMono()
            }
            val daysCount = ChronoUnit.DAYS.between(fromTime, toTime)
            if (daysCount <= 0) {
                return@flatMap ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .build<Flux<GetProductSales200ResponseInner>>().toMono()
            }
            val productSalesAnalytics = productServiceAnalytics.getProductSalesAnalytics(
                productIds,
                fromTime.toLocalDateTime(),
                toTime.toLocalDateTime()
            )
            val productSales = productSalesAnalytics.map {
                GetProductSales200ResponseInner().apply {
                    this.productId = it.productId.toLong()
                    this.salesAmount = it.salesAmount.toDouble()
                    this.orderAmount = it.orderAmount
                    this.dailyOrder = it.dailyOrderAmount.setScale(2, RoundingMode.HALF_UP).toDouble()
                    this.seller = Seller().apply {
                        this.accountId = it.sellerAccountId
                        this.link = it.sellerLink
                        this.title = it.sellerTitle
                    }
                }
            }
            ResponseEntity.ok(productSales.toFlux()).toMono()
        }
    }

    override fun getSellerShops(
        sellerLink: String,
        exchange: ServerWebExchange
    ): Mono<ResponseEntity<Flux<Seller>>> {
        return ResponseEntity.ok(sellerService.findSellersByLink(sellerLink).map {
            Seller().apply {
                this.title = it.title
                this.link = it.link
                this.accountId = it.accountId
            }
        }.toFlux()).toMono().doOnError { log.error(it) { "Failed to get seller shops" } }
    }

    override fun getReportByReportId(reportId: String, exchange: ServerWebExchange): Mono<ResponseEntity<Resource>> =
        runBlocking<ResponseEntity<Resource>> {
            val report =
                reportFileService.getReport(reportId) ?: return@runBlocking ResponseEntity.notFound().build<Resource>()
            val reportData = withContext(Dispatchers.IO) {
                report.stream.readAllBytes()
            }

            return@runBlocking ResponseEntity.ok()
                .contentType(MediaType.APPLICATION_OCTET_STREAM)
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"${report.name}.xlsx\"")
                .body(InputStreamResource(reportData.inputStream()))
        }.toMono().doOnError { log.error(it) { "Failed get report by report id" } }

    override fun getReportBySeller(
        sellerLink: String,
        period: Int,
        idempotenceKey: String,
        exchange: ServerWebExchange
    ): Mono<ResponseEntity<GetReportBySeller200Response>> {
        val apiKey = exchange.request.headers["X-API-KEY"]?.first()
            ?: return ResponseEntity.badRequest().build<GetReportBySeller200Response>().toMono()
        val idempotenceKey = exchange.request.headers["Idempotence-Key"]?.first()
            ?: return ResponseEntity.badRequest().build<GetReportBySeller200Response>().toMono()
        val user = userRepository.findByApiKey_HashKey(apiKey)
            ?: return ResponseEntity.badRequest().build<GetReportBySeller200Response>().toMono()
        // Check report period permission
        val checkDaysAccess = userRestrictionService.checkDaysAccess(user, period)
        if (checkDaysAccess == UserRestrictionService.RestrictionResult.PROHIBIT) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).build<GetReportBySeller200Response>().toMono()
        }

        // Check daily report count permission
        val reportCount =
            reportService.getUserShopReportDailyReportCountV2(user.userId)

        val checkReportAccess =
            userRestrictionService.checkShopReportAccess(user, reportCount)
        if (checkReportAccess == UserRestrictionService.RestrictionResult.PROHIBIT) {
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                .build<GetReportBySeller200Response>().toMono()
        }

        // Save report job
        val report = reportRepository.findByRequestIdAndSellerLink(idempotenceKey, sellerLink)
        return if (report != null) {
            ResponseEntity.ok().body(GetReportBySeller200Response().apply {
                this.reportId = report.reportId
                this.jobId = UUID.fromString(report.jobId)
            }).toMono()
        } else {
            val jobId = UUID.randomUUID().toString()
            reportRepository.saveNewSellerReport(
                Reports(
                    idempotenceKey,
                    jobId,
                    user.userId,
                    period,
                    LocalDateTime.now(),
                    sellerLink,
                    null,
                    dev.crashteam.keanalytics.db.model.enums.ReportType.seller,
                    dev.crashteam.keanalytics.db.model.enums.ReportStatus.processing,
                    null
                )
            )
            ResponseEntity.ok().body(GetReportBySeller200Response().apply {
                this.jobId = UUID.fromString(jobId)
            }).toMono()
        }
    }

    override fun getReportByCategory(
        categoryId: Long,
        period: Int,
        idempotenceKey: String,
        exchange: ServerWebExchange
    ): Mono<ResponseEntity<GetReportBySeller200Response>> {
        val apiKey = exchange.request.headers["X-API-KEY"]?.first()
            ?: return ResponseEntity.badRequest().build<GetReportBySeller200Response>().toMono()
        val idempotenceKey = exchange.request.headers["Idempotence-Key"]?.first()
            ?: return ResponseEntity.badRequest().build<GetReportBySeller200Response>().toMono()
        val user = userRepository.findByApiKey_HashKey(apiKey)
            ?: return ResponseEntity.badRequest().build<GetReportBySeller200Response>().toMono()
        // Check report period permission
        val checkDaysAccess = userRestrictionService.checkDaysAccess(user, period)
        if (checkDaysAccess == UserRestrictionService.RestrictionResult.PROHIBIT) {
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                .build<GetReportBySeller200Response>().toMono()
        }

        // Check daily report count permission
        val userReportDailyReportCount =
            reportService.getUserCategoryReportDailyReportCountV2(user.userId)
        val checkReportAccess = userRestrictionService.checkCategoryReportAccess(
            user, userReportDailyReportCount
        )
        if (checkReportAccess == UserRestrictionService.RestrictionResult.PROHIBIT) {
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                .build<GetReportBySeller200Response>().toMono()
        }

        val jobId = UUID.randomUUID().toString()
        val report =
            reportRepository.findByRequestIdAndCategoryPublicId(idempotenceKey, categoryId)
        if (report != null) {
            return ResponseEntity.ok().body(GetReportBySeller200Response().apply {
                this.jobId = UUID.fromString(jobId)
                this.reportId = report.reportId
            }).toMono()
        } else {
            reportRepository.saveNewCategoryReport(
                Reports(
                    idempotenceKey,
                    jobId,
                    user.userId,
                    period,
                    LocalDateTime.now(),
                    null,
                    categoryId.toString(),
                    dev.crashteam.keanalytics.db.model.enums.ReportType.category,
                    dev.crashteam.keanalytics.db.model.enums.ReportStatus.processing,
                    null
                )
            )
            return ResponseEntity.ok().body(GetReportBySeller200Response().apply {
                this.jobId = UUID.fromString(jobId)
            }).toMono()
        }
    }

    override fun getReportStateByJobId(
        jobId: UUID,
        exchange: ServerWebExchange
    ): Mono<ResponseEntity<GetReportStateByJobId200Response>> {
        val report = reportRepository.findByJobId(jobId.toString())
        if (report != null) {
            val responseBody = GetReportStateByJobId200Response().apply {
                this.reportId = report.reportId
                this.jobId = UUID.fromString(report.jobId)
                this.status = report.status.name.lowercase()
                this.interval = report.interval ?: -1
                this.reportType = report.reportType?.name ?: "unknown"
                this.createdAt = report.createdAt.atOffset(ZoneOffset.UTC)
                this.sellerLink = report.sellerLink
                this.categoryId = report.categoryId?.toLong()
            }

            return ResponseEntity.ok(responseBody).toMono()
        } else {
            return ResponseEntity.notFound().build<GetReportStateByJobId200Response>().toMono()
        }
    }

    override fun getReports(
        fromTime: OffsetDateTime,
        exchange: ServerWebExchange
    ): Mono<ResponseEntity<Flux<Report>>> {
        val apiKey = exchange.request.headers["X-API-KEY"]?.first()
            ?: return ResponseEntity.badRequest().build<Flux<Report>>().toMono()
        val user = userRepository.findByApiKey_HashKey(apiKey)
            ?: return ResponseEntity.badRequest().build<Flux<Report>>().toMono()
        val reports =
            reportRepository.findByUserIdAndCreatedAtFromTime(user.userId, fromTime.toLocalDateTime()).map {
                Report().apply {
                    reportId = it.reportId
                    jobId = UUID.fromString(it.jobId)
                    status = it.status.name.lowercase()
                    interval = it.interval ?: -1
                    reportType = it.reportType?.name ?: "unknown"
                    createdAt = it.createdAt.atOffset(ZoneOffset.UTC)
                    sellerLink = it.sellerLink
                    categoryId = it.categoryId.toLong()
                }
            }
        if (reports.isEmpty()) {
            return ResponseEntity.notFound().build<Flux<Report>>().toMono()
        } else {
            return ResponseEntity.ok(reports.toFlux()).toMono()
        }
    }

    override fun categoryOverallInfo(
        xRequestID: UUID,
        X_API_KEY: String,
        categoryId: Long,
        fromTime: OffsetDateTime,
        toTime: OffsetDateTime,
        exchange: ServerWebExchange
    ): Mono<ResponseEntity<CategoryOverallInfo200Response>> {
        val fromTimeLocalDateTime = fromTime.toLocalDateTime() ?: LocalDate.now().minusDays(30).atTime(LocalTime.MIN)
        val toTimeLocalDateTime = toTime.toLocalDateTime() ?: LocalDate.now().atTime(LocalTime.MAX)
        return checkRequestDaysPermission(X_API_KEY, fromTimeLocalDateTime, toTimeLocalDateTime).flatMap { access ->
            if (access == false) {
                ResponseEntity.status(HttpStatus.FORBIDDEN).build<CategoryOverallInfo200Response>().toMono()
            } else {
                val categoryOverallAnalytics = productServiceAnalytics.getCategoryOverallAnalytics(
                    categoryId,
                    fromTimeLocalDateTime,
                    toTimeLocalDateTime
                ) ?: return@flatMap ResponseEntity.notFound().build<CategoryOverallInfo200Response>().toMono()
                return@flatMap ResponseEntity.ok(CategoryOverallInfo200Response().apply {
                    this.averagePrice =
                        categoryOverallAnalytics.averagePrice.setScale(2, RoundingMode.HALF_UP).toDouble()
                    this.revenue = categoryOverallAnalytics.revenue?.setScale(2, RoundingMode.HALF_UP)?.toDouble()
                    this.orderCount = categoryOverallAnalytics.orderCount
                    this.sellerCount = categoryOverallAnalytics.sellerCount
                    this.salesPerSeller =
                        categoryOverallAnalytics.salesPerSeller.setScale(2, RoundingMode.HALF_UP).toDouble()
                    this.productCount = categoryOverallAnalytics.productCount
                    this.productZeroSalesCount = categoryOverallAnalytics.productZeroSalesCount
                    this.sellersZeroSalesCount = categoryOverallAnalytics.sellersZeroSalesCount
                }).toMono()
            }
        }
    }

    @ExperimentalCoroutinesApi
    override fun sellerOverallInfo(
        xRequestID: UUID,
        X_API_KEY: String,
        sellerLink: String,
        fromTime: OffsetDateTime,
        toTime: OffsetDateTime,
        exchange: ServerWebExchange
    ): Mono<ResponseEntity<SellerOverallInfo200Response>> {
        val fromTimeLocalDateTime = fromTime.toLocalDateTime()
        val toTimeLocalDateTime = toTime.toLocalDateTime()
        return checkRequestDaysPermission(X_API_KEY, fromTimeLocalDateTime, toTimeLocalDateTime).flatMap { access ->
            if (access == false) {
                ResponseEntity.status(HttpStatus.FORBIDDEN).build<SellerOverallInfo200Response>().toMono()
            } else {
                val categoryOverallAnalytics =
                    productServiceAnalytics.getSellerAnalytics(sellerLink, fromTimeLocalDateTime, toTimeLocalDateTime)
                        ?: return@flatMap ResponseEntity.notFound().build<SellerOverallInfo200Response>().toMono()
                return@flatMap ResponseEntity.ok(SellerOverallInfo200Response().apply {
                    this.averagePrice =
                        categoryOverallAnalytics.averagePrice.setScale(2, RoundingMode.HALF_UP).toDouble()
                    this.orderCount = categoryOverallAnalytics.orderCount
                    this.productCount = categoryOverallAnalytics.productCount
                    this.revenue = categoryOverallAnalytics.revenue.setScale(2, RoundingMode.HALF_UP).toDouble()
                    this.productCountWithSales = categoryOverallAnalytics.productCountWithSales
                    this.salesDynamic = categoryOverallAnalytics.salesDynamic.map { chSellerOrderDynamic ->
                        DynamicSales().apply {
                            date = chSellerOrderDynamic.date
                            orderAmount = chSellerOrderDynamic.orderAmount
                        }
                    }
                    this.productCountWithoutSales = categoryOverallAnalytics.productWithoutSales
                }).toMono()
            }
        }
    }

    override fun giveawayDemoSubscription(
        xRequestID: UUID,
        giveawayUserDemoRequest: Mono<GiveawayUserDemoRequest>,
        exchange: ServerWebExchange
    ): Mono<ResponseEntity<Void>> {
        return exchange.getPrincipal<Principal>().flatMap {
            val user = userRepository.findByUserId(it.name)!!
            giveawayUserDemoRequest.flatMap { giveawayUserDemoRequest ->
                if (user.role != "ADMIN") {
                    ResponseEntity.status(HttpStatus.FORBIDDEN).build<Void>().toMono()
                } else {
                    try {
                        userSubscriptionService.giveawayDemoSubscription(giveawayUserDemoRequest.userId, 3)
                        ResponseEntity.ok().build<Void>().toMono()
                    } catch (e: UserSubscriptionGiveawayException) {
                        ResponseEntity.badRequest().build<Void>().toMono()
                    }
                }
            }
        }.doOnError { log.error(it) { "Failed to giveaway demo for user" } }
    }

    override fun categoryProductPosition(
        xRequestID: UUID,
        X_API_KEY: String,
        categoryId: Long,
        productId: Long,
        skuId: Long,
        fromDate: LocalDate,
        toDate: LocalDate,
        exchange: ServerWebExchange
    ): Mono<ResponseEntity<CategoryProductPosition200Response>> {
        val productPositions = productService.getProductPosition(
            categoryId,
            productId,
            skuId,
            fromDate.atStartOfDay(),
            toDate.atStartOfDay()
        )
        return ResponseEntity.ok(CategoryProductPosition200Response().apply {
            this.categoryId = categoryId
            this.productId = productId
            this.skuId = skuId
            this.positions = productPositions.map {
                CategoryProductPosition().apply {
                    this.date = it.date
                    this.position = it.position.toInt()
                }
            }
        }).toMono()
    }

    private fun checkRequestDaysPermission(
        apiKey: String,
        fromTime: LocalDateTime,
        toTime: LocalDateTime
    ): Mono<Boolean> {
        val daysCount = ChronoUnit.DAYS.between(fromTime, toTime)
        if (daysCount <= 0) return true.toMono()
        val user = userRepository.findByApiKey_HashKey(apiKey)!!
        val checkDaysAccess = userRestrictionService.checkDaysAccess(user, daysCount.toInt())
        if (checkDaysAccess == UserRestrictionService.RestrictionResult.PROHIBIT) {
            return false.toMono()
        }
        val checkDaysHistoryAccess = userRestrictionService.checkDaysHistoryAccess(user, fromTime)
        if (checkDaysHistoryAccess == UserRestrictionService.RestrictionResult.PROHIBIT) {
            return false.toMono()
        }
        return true.toMono()
    }

}
