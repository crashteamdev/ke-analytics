package dev.crashteam.keanalytics.controller

import dev.crashteam.openapi.keanalytics.api.*
import dev.crashteam.openapi.keanalytics.model.*
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import mu.KotlinLogging
import dev.crashteam.keanalytics.domain.mongo.ReportDocument
import dev.crashteam.keanalytics.domain.mongo.ReportStatus
import dev.crashteam.keanalytics.domain.mongo.ReportType
import dev.crashteam.keanalytics.domain.mongo.ReportVersion
import dev.crashteam.keanalytics.report.ReportFileService
import dev.crashteam.keanalytics.report.ReportService
import dev.crashteam.keanalytics.repository.mongo.CategoryRepository
import dev.crashteam.keanalytics.repository.mongo.ReportRepository
import dev.crashteam.keanalytics.repository.mongo.UserRepository
import dev.crashteam.keanalytics.service.CategoryService
import dev.crashteam.keanalytics.service.ProductServiceV2
import dev.crashteam.keanalytics.service.SellerService
import dev.crashteam.keanalytics.service.UserRestrictionService
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
import java.time.*
import java.util.*

private val log = KotlinLogging.logger {}

@RestController
@RequestMapping(path = ["v2"], produces = [MediaType.APPLICATION_JSON_VALUE])
class MarketDbApiV2Controller(
    private val categoryService: CategoryService,
    private val productServiceV2: ProductServiceV2,
    private val sellerService: SellerService,
    private val userRepository: UserRepository,
    private val userRestrictionService: UserRestrictionService,
    private val reportFileService: ReportFileService,
    private val reportService: ReportService,
    private val reportRepository: ReportRepository,
    private val categoryRepository: CategoryRepository,
) : CategoryApi, ProductApi, SellerApi, ReportApi, ReportsApi {

    override fun categoryAveragePrice(
        xRequestID: String,
        path: MutableList<String>,
        day: LocalDate,
        exchange: ServerWebExchange
    ): Mono<ResponseEntity<CategoryAveragePrice200Response>> {
        return categoryService.getCategoryAveragePrice(path, day).flatMap { categoryAveragePrice ->
            ResponseEntity.ok(CategoryAveragePrice200Response().apply {
                this.averagePrice = categoryAveragePrice.averagePrice.toLong()
                this.productCount = categoryAveragePrice.productCount
            }).toMono()
        }.toMono().doOnError { log.error(it) { "Failed get category average price" } }
    }

    override fun productSkuHistory(
        xRequestID: String,
        productId: Long,
        skuId: Long,
        fromTime: OffsetDateTime,
        toTime: OffsetDateTime,
        limit: Int,
        offset: Int,
        exchange: ServerWebExchange
    ): Mono<ResponseEntity<Flux<ProductSkuHistory>>> {
        return productServiceV2.getProductSkuSalesHistory(productId, skuId, fromTime, toTime, limit, offset).flatMap {
            val productSkuHistories = it.data.map { productSkuHistory ->
                ProductSkuHistory().apply {
                    this.productId = productSkuHistory.productId
                    this.skuId = productSkuHistory.skuId
                    this.name = productSkuHistory.name
                    this.orderAmount = productSkuHistory.orderAmount
                    this.reviewsAmount = productSkuHistory.reviewsAmount
                    this.totalAvailableAmount = productSkuHistory.totalAvailableAmount
                    this.fullPrice = productSkuHistory.fullPrice.toDouble()
                    this.purchasePrice = productSkuHistory.price.toDouble()
                    this.availableAmount = productSkuHistory.availableAmount
                    this.photoKey = productSkuHistory.photoKey
                    this.date = productSkuHistory.id.day.toInstant().atZone(ZoneId.of("UTC")).toLocalDate()
                    this.salesAmount = productSkuHistory.salesAmount.setScale(2, RoundingMode.HALF_UP).toDouble()
                }
            }.toFlux()
            ResponseEntity.ok(productSkuHistories).toMono()
        }.toMono().doOnError { log.error(it) { "Failed get product sku history" } }
    }

    override fun getProductSales(
        productIds: MutableList<Long>,
        fromTime: OffsetDateTime,
        toTime: OffsetDateTime,
        exchange: ServerWebExchange
    ): Mono<ResponseEntity<Flux<GetProductSales200ResponseInner>>> {
        return ResponseEntity.ok(
            productServiceV2.getProductsSales(productIds.toLongArray(), fromTime, toTime).map {
                GetProductSales200ResponseInner().apply {
                    this.productId = it.id.productId
                    this.salesAmount = it.salesAmount.setScale(2, RoundingMode.HALF_UP).toDouble()
                    this.orderAmount = it.orderAmount
                    this.dailyOrder = it.dailyOrder.setScale(2, RoundingMode.HALF_UP).toDouble()
                    this.seller = Seller().apply {
                        this.title = it.sellerTitle
                        this.link = it.sellerLink
                        this.accountId = it.sellerAccountId
                    }
                }
            }).toMono().doOnError { log.error(it) { "Failed get products sales history" } }
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
                                reportService.incrementShopUserReportCountV2(userDocument.userId).subscribe()
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
                                    reportService.incrementCategoryUserReportCountV2(userDocument.userId).subscribe()
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
}
