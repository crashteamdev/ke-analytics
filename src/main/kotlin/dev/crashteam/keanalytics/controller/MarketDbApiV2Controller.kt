package dev.crashteam.keanalytics.controller

import dev.crashteam.keanalytics.domain.mongo.*
import dev.crashteam.keanalytics.report.ReportFileService
import dev.crashteam.keanalytics.report.ReportService
import dev.crashteam.keanalytics.repository.mongo.CategoryRepository
import dev.crashteam.keanalytics.repository.mongo.ReportRepository
import dev.crashteam.keanalytics.repository.mongo.UserRepository
import dev.crashteam.keanalytics.service.ProductServiceAnalytics
import dev.crashteam.keanalytics.service.PromoCodeService
import dev.crashteam.keanalytics.service.SellerService
import dev.crashteam.keanalytics.service.UserRestrictionService
import dev.crashteam.keanalytics.service.model.PromoCodeCreateData
import dev.crashteam.openapi.keanalytics.api.*
import dev.crashteam.openapi.keanalytics.model.*
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import mu.KotlinLogging
import org.springframework.core.convert.ConversionService
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
import reactor.core.publisher.toMono
import reactor.kotlin.core.publisher.toFlux
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
    private val categoryRepository: CategoryRepository,
    private val productServiceAnalytics: ProductServiceAnalytics,
    private val promoCodeService: PromoCodeService,
    private val conversionService: ConversionService,
) : CategoryApi, ProductApi, SellerApi, ReportApi, ReportsApi, PromoCodeApi {

    override fun productSkuHistory(
        xRequestID: String,
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
        xRequestID: String,
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

    override fun getSellerShops(sellerLink: String, exchange: ServerWebExchange): Mono<ResponseEntity<Flux<Seller>>> {
        return ResponseEntity.ok(sellerService.findSellersByLink(sellerLink).map {
            Seller().apply {
                this.title = it.title
                this.link = it.link
                this.accountId = it.accountId
            }
        }).toMono().doOnError { log.error(it) { "Failed to get seller shops" } }
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
        return userRepository.findByApiKey_HashKey(apiKey).flatMap { userDocument ->
            // Check report period permission
            val checkDaysAccess = userRestrictionService.checkDaysAccess(userDocument, period)
            if (checkDaysAccess == UserRestrictionService.RestrictionResult.PROHIBIT) {
                return@flatMap ResponseEntity.status(HttpStatus.FORBIDDEN).build<GetReportBySeller200Response>()
                    .toMono()
            }

            // Check daily report count permission
            return@flatMap reportService.getUserShopReportDailyReportCountV2(userDocument.userId).defaultIfEmpty(0)
                .flatMap { reportCount ->
                    val checkReportAccess =
                        userRestrictionService.checkShopReportAccess(userDocument, reportCount?.toInt() ?: 0)
                    if (checkReportAccess == UserRestrictionService.RestrictionResult.PROHIBIT) {
                        return@flatMap ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                            .build<GetReportBySeller200Response>().toMono()
                    }

                    // Save report job
                    return@flatMap reportRepository.findByRequestIdAndSellerLink(idempotenceKey, sellerLink)
                        .flatMap { report ->
                            if (report.status != ReportStatus.FAILED) {
                                return@flatMap ResponseEntity.ok().body(GetReportBySeller200Response().apply {
                                    this.reportId = report.reportId
                                    this.jobId = UUID.fromString(report.jobId)
                                }).toMono()
                            }
                            Mono.create { ResponseEntity.badRequest().build<GetReportBySeller200Response>() }
                        }.switchIfEmpty(Mono.defer {
                            val jobId = UUID.randomUUID().toString()
                            reportRepository.save(
                                ReportDocument(
                                    reportId = null,
                                    requestId = idempotenceKey,
                                    jobId = jobId,
                                    userId = userDocument.userId,
                                    period = null,
                                    interval = period,
                                    createdAt = LocalDateTime.now(),
                                    sellerLink = sellerLink,
                                    categoryPublicId = null,
                                    reportType = ReportType.SELLER,
                                    status = ReportStatus.PROCESSING,
                                    version = ReportVersion.V2
                                )
                            ).flatMap {
                                ResponseEntity.ok().body(GetReportBySeller200Response().apply {
                                    this.jobId = UUID.fromString(jobId)
                                }).toMono()
                            }
                        })
                }
        }.doOnError { log.error(it) { "Failed to generate report by seller" } }
    }

    override fun getReportByCategory(
        path: MutableList<Long>,
        period: Int,
        idempotenceKey: String,
        exchange: ServerWebExchange
    ): Mono<ResponseEntity<GetReportBySeller200Response>> {
        val apiKey = exchange.request.headers["X-API-KEY"]?.first()
            ?: return ResponseEntity.badRequest().build<GetReportBySeller200Response>().toMono()
        val idempotenceKey = exchange.request.headers["Idempotence-Key"]?.first()
            ?: return ResponseEntity.badRequest().build<GetReportBySeller200Response>().toMono()
        return userRepository.findByApiKey_HashKey(apiKey).flatMap { userDocument ->
            // Check report period permission
            val checkDaysAccess = userRestrictionService.checkDaysAccess(userDocument, period)
            if (checkDaysAccess == UserRestrictionService.RestrictionResult.PROHIBIT) {
                return@flatMap ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                    .build<GetReportBySeller200Response>().toMono()
            }

            // Check daily report count permission
            return@flatMap reportService.getUserCategoryReportDailyReportCountV2(userDocument.userId).defaultIfEmpty(0)
                .flatMap { userReportDailyReportCount ->
                    val checkReportAccess = userRestrictionService.checkCategoryReportAccess(
                        userDocument, userReportDailyReportCount?.toInt() ?: 0
                    )
                    if (checkReportAccess == UserRestrictionService.RestrictionResult.PROHIBIT) {
                        return@flatMap ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                            .build<GetReportBySeller200Response>().toMono()
                    }

                    val categoryDocumentMono = Mono.defer {
                        if (path.size == 1) {
                            categoryRepository.findByPublicId(path.first())
                        } else {
                            val categoriesMonoList = path.map { categoryId ->
                                categoryRepository.findByPublicId(categoryId)
                            }
                            Flux.concat(categoriesMonoList).collectList().flatMap { categories ->
                                val sb = StringBuilder()
                                for (category in categories) {
                                    sb.append(",${category.title}")
                                }
                                sb.append(",")
                                sb.toString().toMono()
                            }.flatMap { titlePath ->
                                categoryRepository.findByPath(titlePath).next()
                            }
                        }
                    }
                    categoryDocumentMono.flatMap { categoryDocument ->
                        val jobId = UUID.randomUUID().toString()
                        reportRepository.findByRequestIdAndCategoryPublicId(idempotenceKey, categoryDocument.publicId)
                            .flatMap { report ->
                                if (report.status != ReportStatus.FAILED) {
                                    return@flatMap ResponseEntity.ok().body(GetReportBySeller200Response().apply {
                                        this.jobId = UUID.fromString(jobId)
                                        this.reportId = report.reportId
                                    }).toMono()
                                }
                                Mono.create { ResponseEntity.badRequest().build<GetReportBySeller200Response>() }
                            }.switchIfEmpty(Mono.defer {
                                val categoryTitlePath = if (categoryDocument.path == null) {
                                    listOf(categoryDocument.title)
                                } else {
                                    categoryDocument.path.split(",").filter { it.isNotEmpty() }
                                }
                                reportRepository.save(
                                    ReportDocument(
                                        reportId = null,
                                        requestId = idempotenceKey,
                                        jobId = jobId,
                                        userId = userDocument.userId,
                                        period = null,
                                        interval = period,
                                        createdAt = LocalDateTime.now(),
                                        sellerLink = null,
                                        categoryPublicId = categoryDocument.publicId,
                                        categoryPath = categoryTitlePath,
                                        reportType = ReportType.CATEGORY,
                                        status = ReportStatus.PROCESSING,
                                        version = ReportVersion.V2
                                    )
                                ).flatMap {
                                    ResponseEntity.ok().body(GetReportBySeller200Response().apply {
                                        this.jobId = UUID.fromString(jobId)
                                    }).toMono()
                                }
                            })
                    }.switchIfEmpty(Mono.defer {
                        log.warn { "Failed to generate report by category. Category not found" }
                        ResponseEntity.badRequest().build<GetReportBySeller200Response>().toMono()
                    })
                }
        }.doOnError { log.error(it) { "Failed to generate report by category" } }
    }

    override fun getReportStateByJobId(
        jobId: UUID,
        exchange: ServerWebExchange
    ): Mono<ResponseEntity<GetReportStateByJobId200Response>> {
        return reportRepository.findByJobId(jobId.toString()).flatMap { reportDocument: ReportDocument ->
            val responseBody = GetReportStateByJobId200Response().apply {
                this.reportId = reportDocument.reportId
                this.jobId = UUID.fromString(reportDocument.jobId)
                this.status = reportDocument.status.name.lowercase()
                this.interval = reportDocument.interval ?: -1
                this.reportType = reportDocument.reportType?.name ?: "unknown"
                this.createdAt = reportDocument.createdAt.atOffset(ZoneOffset.UTC)
                this.sellerLink = reportDocument.sellerLink
                this.categoryId = reportDocument.categoryPublicId
            }

            return@flatMap ResponseEntity.ok(responseBody).toMono()
        }.switchIfEmpty(ResponseEntity.notFound().build<GetReportStateByJobId200Response>().toMono())
            .doOnError { log.error(it) { "Failed get report by jobId. JobId=$jobId" } }
    }

    override fun getReports(
        fromTime: OffsetDateTime,
        exchange: ServerWebExchange
    ): Mono<ResponseEntity<Flux<Report>>> {
        val apiKey = exchange.request.headers["X-API-KEY"]?.first()
            ?: return ResponseEntity.badRequest().build<Flux<Report>>().toMono()
        return userRepository.findByApiKey_HashKey(apiKey).flatMap { userDocument ->
            return@flatMap reportRepository.findByUserIdAndCreatedAtFromTime(
                userDocument.userId,
                fromTime.toLocalDateTime()
            )
                .map { reportDoc ->
                    Report().apply {
                        reportId = reportDoc.reportId
                        jobId = UUID.fromString(reportDoc.jobId)
                        status = reportDoc.status.name.lowercase()
                        interval = reportDoc.interval ?: -1
                        reportType = reportDoc.reportType?.name ?: "unknown"
                        createdAt = reportDoc.createdAt.atOffset(ZoneOffset.UTC)
                        sellerLink = reportDoc.sellerLink
                        categoryId = reportDoc.categoryPublicId
                    }
                }.collectList()
        }.switchIfEmpty(emptyList<Report>().toMono()).flatMap { reportDocuments ->
            if (reportDocuments.isNullOrEmpty()) {
                return@flatMap ResponseEntity.notFound().build<Flux<Report>>().toMono()
            }
            ResponseEntity.ok(reportDocuments.toFlux()).toMono()
        }.doOnError { log.error(it) { "Failed get reports" } }
    }

    override fun categoryOverallInfo(
        xRequestID: String,
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
        xRequestID: String,
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
                }).toMono()
            }
        }
    }

    override fun createPromoCode(
        xRequestID: String,
        promoCode: Mono<PromoCode>,
        exchange: ServerWebExchange
    ): Mono<ResponseEntity<PromoCode>> {
        return exchange.getPrincipal<Principal>().flatMap { principal ->
            promoCode.flatMap { promoCode ->
                return@flatMap userRepository.findByUserId(principal.name).flatMap { userDocument ->
                    if (userDocument.role != UserRole.ADMIN) {
                        return@flatMap ResponseEntity.status(HttpStatus.FORBIDDEN).build<PromoCode>().toMono()
                    }
                    promoCodeService.createPromoCode(
                        PromoCodeCreateData(
                            description = promoCode.description,
                            validUntil = promoCode.validUntil.toLocalDateTime(),
                            useLimit = promoCode.useLimit,
                            type = when (promoCode.context.type) {
                                PromoCodeContext.TypeEnum.ADDITIONAL_TIME -> PromoCodeType.ADDITIONAL_DAYS
                                PromoCodeContext.TypeEnum.DISCOUNT -> PromoCodeType.DISCOUNT
                                else -> PromoCodeType.DISCOUNT
                            },
                            discount = if (promoCode.context is DiscountPromoCode) {
                                (promoCode.context as DiscountPromoCode).discount.toShort()
                            } else null,
                            additionalDays = if (promoCode.context is AdditionalTimePromoCode) {
                                (promoCode.context as AdditionalTimePromoCode).additionalDays
                            } else null,
                            prefix = promoCode.prefix,
                        )
                    ).flatMap { promoCodeDocument ->
                        ResponseEntity.ok(conversionService.convert(promoCodeDocument, PromoCode::class.java)).toMono()
                    }
                }
            }
        }
    }

    private fun checkRequestDaysPermission(
        apiKey: String,
        fromTime: LocalDateTime,
        toTime: LocalDateTime
    ): Mono<Boolean> {
        val daysCount = ChronoUnit.DAYS.between(fromTime, toTime)
        if (daysCount <= 0) return true.toMono()
        return userRepository.findByApiKey_HashKey(apiKey).flatMap { user ->
            val checkDaysAccess = userRestrictionService.checkDaysAccess(user, daysCount.toInt())
            if (checkDaysAccess == UserRestrictionService.RestrictionResult.PROHIBIT) {
                return@flatMap false.toMono()
            }
            val checkDaysHistoryAccess = userRestrictionService.checkDaysHistoryAccess(user, fromTime)
            if (checkDaysHistoryAccess == UserRestrictionService.RestrictionResult.PROHIBIT) {
                return@flatMap false.toMono()
            }
            true.toMono()
        }
    }

}
