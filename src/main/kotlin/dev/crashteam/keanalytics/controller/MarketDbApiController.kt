package dev.crashteam.keanalytics.controller

import dev.crashteam.keanalytics.controller.model.*
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import kotlinx.coroutines.withContext
import mu.KotlinLogging
import dev.crashteam.keanalytics.domain.mongo.ReportDocument
import dev.crashteam.keanalytics.domain.mongo.ReportStatus
import dev.crashteam.keanalytics.domain.mongo.ReportType
import dev.crashteam.keanalytics.domain.mongo.ReportVersion
import dev.crashteam.keanalytics.report.ReportFileService
import dev.crashteam.keanalytics.report.ReportService
import dev.crashteam.keanalytics.report.model.ReportJob
import dev.crashteam.keanalytics.repository.mongo.CategoryRepository
import dev.crashteam.keanalytics.repository.mongo.ProductCustomRepositoryImpl
import dev.crashteam.keanalytics.repository.mongo.ReportRepository
import dev.crashteam.keanalytics.repository.mongo.UserRepository
import dev.crashteam.keanalytics.repository.mongo.model.FindProductFilter
import dev.crashteam.keanalytics.repository.mongo.model.ProductTotalOrderAggregate
import dev.crashteam.keanalytics.repository.mongo.model.ShopTotalOrder
import dev.crashteam.keanalytics.repository.mongo.pageable.PageResult
import dev.crashteam.keanalytics.service.CategoryService
import dev.crashteam.keanalytics.service.ProductService
import dev.crashteam.keanalytics.service.UserRestrictionService
import org.springframework.core.convert.ConversionService
import org.springframework.format.annotation.DateTimeFormat
import org.springframework.http.*
import org.springframework.web.bind.annotation.*
import org.springframework.web.client.RestTemplate
import org.springframework.web.client.exchange
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.math.BigDecimal
import java.math.RoundingMode
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import java.util.*
import javax.validation.Valid

private val log = KotlinLogging.logger {}

@RestController
@RequestMapping(path = ["v1"], produces = [MediaType.APPLICATION_JSON_VALUE])
class MarketDbApiController(
    private val productCustomRepository: ProductCustomRepositoryImpl,
    private val reportRepository: ReportRepository,
    private val userRepository: UserRepository,
    private val categoryRepository: CategoryRepository,
    private val productService: ProductService,
    private val categoryService: CategoryService,
    private val conversionService: ConversionService,
    private val reportFileService: ReportFileService,
    private val reportService: ReportService,
    private val userRestrictionService: UserRestrictionService,
    private val restTemplate: RestTemplate,
) {

    @GetMapping("/shop/top")
    suspend fun getTopShopByOrders(): ResponseEntity<Flux<ShopTotalOrder>> {
        return ResponseEntity
            .ok()
            .contentType(MediaType.APPLICATION_JSON)
            .body(productCustomRepository.findTopShopsByTotalOrders())
    }

    @GetMapping("/product/top")
    suspend fun getTopProductByOrders(): ResponseEntity<Flux<ProductTotalOrderAggregate>> {
        return ResponseEntity
            .ok()
            .contentType(MediaType.APPLICATION_JSON)
            .body(productCustomRepository.findTopProductByOrders(100))
    }

    @GetMapping("/product/search")
    suspend fun searchProduct(
        @RequestParam(required = false) category: String?,
        @RequestParam(required = false) seller: String?,
        @RequestParam(name = "seller_link", required = false) sellerLink: String?,
        @RequestParam(name = "product_name", required = false) productName: String?,
        @RequestParam(name = "order_amount_gt", required = false) orderAmountGt: Long?,
        @RequestParam(name = "order_amount_lt", required = false) orderAmountLt: Long?,
        @RequestParam(defaultValue = "0") page: Int,
        @RequestParam(defaultValue = "20") size: Int,
        @RequestParam(defaultValue = "product_id,desc") sort: Array<String>,
    ): ResponseEntity<Mono<PageResult<ProductView>>> {
        val filter = FindProductFilter(
            category = category,
            sellerName = seller,
            sellerLink = sellerLink,
            productName = productName,
            orderAmountGt = orderAmountGt,
            orderAmountLt = orderAmountLt,
        )
        val productsPageResult = productService.findProductByProperties(filter, sort, page, size).map { pageResult ->
            val productViewList = pageResult.result.map {
                conversionService.convert(it, ProductView::class.java)!!
            }
            PageResult(productViewList, pageResult.pageSize, pageResult.page, pageResult.totalPages)
        }

        return ResponseEntity
            .ok()
            .contentType(MediaType.APPLICATION_JSON)
            .body(productsPageResult)
    }

    @GetMapping("/product/{id}")
    suspend fun getProduct(@PathVariable id: String): ResponseEntity<ProductView> {
        val product = productService.getProduct(id.toLong()).map {
            conversionService.convert(it, ProductView::class.java)!!
        }.awaitSingleOrNull() ?: return ResponseEntity.notFound().build()
        return ResponseEntity
            .ok()
            .contentType(MediaType.APPLICATION_JSON)
            .body(product)
    }

    @GetMapping("/product/{product_id}/orders")
    suspend fun getProductSalesInfo(
        @PathVariable(name = "product_id") id: Long,
        @RequestParam(name = "from_time") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) fromTime: LocalDateTime,
        @RequestParam(name = "to_time") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) toTime: LocalDateTime,
        @RequestHeader(name = "X-API-KEY", required = true) apiKey: String,
    ): ResponseEntity<ProductTotalOrdersView> {
        if (!checkRequestDaysPermission(apiKey, fromTime, toTime)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).build()
        }
        val productTotalSales = productService.getProductOrders(id, fromTime, toTime)
            ?: return ResponseEntity.notFound().build()
        val productTotalSalesView = conversionService.convert(productTotalSales, ProductTotalOrdersView::class.java)!!

        return ResponseEntity.ok()
            .contentType(MediaType.APPLICATION_JSON)
            .body(productTotalSalesView)
    }

    @GetMapping("/product/sales/")
    suspend fun getProductSales(
        @RequestParam productIds: LongArray,
        @RequestParam(name = "from_time") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) fromTime: LocalDateTime,
        @RequestParam(name = "to_time") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) toTime: LocalDateTime,
        @RequestHeader(name = "X-API-KEY", required = true) apiKey: String,
    ): ResponseEntity<Map<Long, ProductTotalSalesView>> {
        if (!checkRequestDaysPermission(apiKey, fromTime, toTime)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).build()
        }
        val userDocument = userRepository.findByApiKey_HashKey(apiKey).awaitSingleOrNull()
        val limitProductIds = if (productIds.size > 60) {
            log.warn { "Too match productIds on product sales endpoint. userId=${userDocument?.userId}" }
            productIds.take(60).toLongArray()
        } else productIds
        val productsSales = productService.getProductsSales(limitProductIds, fromTime, toTime)
        if (productsSales == null) {
            log.warn { "Not found product sales for productIds=$limitProductIds; fromTime=$fromTime; toTime=$toTime" }
            return ResponseEntity.notFound().build()
        }
        val groupedByProductId: Map<Long, List<ProductSkuTotalSalesView>> = productsSales.groupBy(
            { it.productId },
            {
                if (it.dayChange.isEmpty()) {
                    ProductSkuTotalSalesView(
                        it.productId,
                        it.skuId,
                        BigDecimal.ZERO,
                        0,
                        it.seller.let { ProductTotalSalesSeller(it.title, it.link, it.accountId) })
                } else {
                    val totalSalesAmount =
                        it.dayChange.map { dayChange -> dayChange.salesAmount }.reduce { a, b -> a + b }
                    val totalOrderAmount =
                        it.dayChange.map { dayChange -> dayChange.orderAmount }.reduce { a, b -> a + b }
                    ProductSkuTotalSalesView(
                        it.productId,
                        it.skuId,
                        totalSalesAmount,
                        totalOrderAmount,
                        it.seller.let { ProductTotalSalesSeller(it.title, it.link, it.accountId) })
                }
            }
        )
        val productTotalSales = groupedByProductId.mapValues {
            val totalSalesAmount = it.value.map { it.salesAmount }.reduce { a, b -> a + b }
            val totalOrderAmount = it.value.map { it.orderAmount }.reduce { a, b -> a + b }
            val daysBetween = ChronoUnit.DAYS.between(fromTime, toTime)
            val dailyOrder = BigDecimal.valueOf(totalOrderAmount)
                .divide(BigDecimal.valueOf(daysBetween), 2, RoundingMode.HALF_UP)
            ProductTotalSalesView(it.key, totalSalesAmount, totalOrderAmount, dailyOrder, it.value[0].seller)
        }
        return ResponseEntity.ok(productTotalSales)
    }

    @GetMapping("/product/{product_id}/sku/{sku_id}/sales")
    suspend fun getProductSales(
        @PathVariable(name = "product_id") id: Long,
        @PathVariable(name = "sku_id") skuId: Long,
        @RequestParam(name = "from_time") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) fromTime: LocalDateTime,
        @RequestParam(name = "to_time") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) toTime: LocalDateTime,
        @RequestParam(defaultValue = "0") page: Int,
        @RequestParam(defaultValue = "20") size: Int,
        @RequestHeader(name = "X-API-KEY", required = true) apiKey: String,
    ): ResponseEntity<PageResult<ProductSkuHistoricalView>> {
        if (!checkRequestDaysPermission(apiKey, fromTime, toTime)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).build()
        }
        val pageResult = productService.getProductSkuSalesHistory(id, skuId, fromTime, toTime, page, size)
        if (pageResult == null) {
            log.warn { "Not found history for productId=$id; skuId=$skuId; fromTime=$fromTime; toTime=$toTime" }
            return ResponseEntity.notFound().build()
        }
        val productSalesView =
            pageResult.result.map { conversionService.convert(it, ProductSkuHistoricalView::class.java)!! }

        return ResponseEntity
            .ok()
            .contentType(MediaType.APPLICATION_JSON)
            .body(PageResult(productSalesView, pageResult.pageSize, pageResult.page))
    }

    @GetMapping("/category/sales")
    suspend fun getCategorySales(
        @RequestParam(name = "ids") ids: List<Long>,
        @RequestParam(name = "from_time") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) fromTime: LocalDateTime,
        @RequestParam(name = "to_time") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) toTime: LocalDateTime,
        @RequestParam(defaultValue = "0") page: Int,
        @RequestParam(defaultValue = "20") size: Int,
        @RequestParam(defaultValue = "sku_id,desc") sort: Array<String>,
        @RequestHeader(name = "X-API-KEY", required = true) apiKey: String,
    ): ResponseEntity<CategorySalesViewWrapper> {
        if (!checkRequestDaysPermission(apiKey, fromTime, toTime)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).build()
        }
        val categorySales = productService.getCategorySales(ids, fromTime, toTime, sort, page, size).awaitSingleOrNull()
            ?: return ResponseEntity.notFound().build()
        val categorySalesViewWrapper = conversionService.convert(categorySales, CategorySalesViewWrapper::class.java)
        return ResponseEntity
            .ok()
            .contentType(MediaType.APPLICATION_JSON)
            .body(categorySalesViewWrapper)
    }

    @GetMapping("/seller/sales")
    suspend fun getSellerSales(
        @RequestParam(name = "title", required = false) title: String?,
        @RequestParam(name = "link", required = false) link: String?,
        @RequestParam(name = "from_time") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) fromTime: LocalDateTime,
        @RequestParam(name = "to_time") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) toTime: LocalDateTime,
        @RequestParam(defaultValue = "0") page: Int,
        @RequestParam(defaultValue = "20") size: Int,
        @RequestParam(defaultValue = "sku_id,desc") sort: Array<String>,
        @RequestHeader(name = "X-API-KEY", required = true) apiKey: String,
    ): ResponseEntity<CategorySalesViewWrapper> {
        if (!checkRequestDaysPermission(apiKey, fromTime, toTime)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).build()
        }
        if (title == null && link == null) {
            return ResponseEntity.badRequest().build()
        }
        val categorySales =
            productService.getSellerSalesPageable(title, link, fromTime, toTime, sort, page, size).awaitSingleOrNull()
                ?: return ResponseEntity.notFound().build()
        val categorySalesViewWrapper = conversionService.convert(categorySales, CategorySalesViewWrapper::class.java)
        return ResponseEntity
            .ok()
            .contentType(MediaType.APPLICATION_JSON)
            .body(categorySalesViewWrapper)
    }

    @GetMapping("/categories")
    suspend fun getCategories(): ResponseEntity<Flux<CategoryView>> {
        val categories = categoryService.getAllCategories().map {
            conversionService.convert(it, CategoryView::class.java)!!
        }
        return ResponseEntity
            .ok()
            .contentType(MediaType.APPLICATION_JSON)
            .body(categories)
    }

    @GetMapping("/category/products")
    suspend fun getCategoryProductIds(@RequestParam(name = "ids") ids: List<Long>): ResponseEntity<List<Long>> {
        val categoryProducts = productService.getCategoryProductIds(ids).collectList().awaitSingleOrNull()
            ?: return ResponseEntity.notFound().build()

        return ResponseEntity
            .ok()
            .contentType(MediaType.APPLICATION_JSON)
            .body(categoryProducts)
    }

    @GetMapping("/category/sellers")
    suspend fun getCategorySellerIds(@RequestParam(name = "ids") ids: List<Long>): ResponseEntity<List<Long>> {
        val categoryProducts = productService.getCategorySellerIds(ids).collectList().awaitSingleOrNull()
            ?: return ResponseEntity.notFound().build()

        return ResponseEntity
            .ok()
            .contentType(MediaType.APPLICATION_JSON)
            .body(categoryProducts)
    }

    @GetMapping("/seller/products")
    suspend fun getSellerProducts(@RequestParam sellerLink: String): ResponseEntity<List<Long>> {
        val sellerProductIds = productService.getSellerProductIdsByLink(sellerLink).collectList().awaitSingleOrNull()
            ?: return ResponseEntity.notFound().build()

        return ResponseEntity
            .ok()
            .contentType(MediaType.APPLICATION_JSON)
            .body(sellerProductIds)
    }

    @GetMapping("/report/seller/{link}")
    suspend fun generateReportBySeller(
        @PathVariable(name = "link") sellerLink: String,
        @RequestParam(name = "period") daysPeriod: Int,
        @RequestHeader(name = "X-API-KEY", required = true) apiKey: String,
        @RequestHeader(name = "Idempotence-Key", required = true) idempotenceKey: String
    ): ResponseEntity<ReportJob> {
        val user = userRepository.findByApiKey_HashKey(apiKey).awaitSingleOrNull()
            ?: throw IllegalStateException("User not found")

        // Check report period permission
        val checkDaysAccess = userRestrictionService.checkDaysAccess(user, daysPeriod)
        if (checkDaysAccess == UserRestrictionService.RestrictionResult.PROHIBIT) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).build()
        }

        // Check daily report count permission
        val userReportDailyReportCount = reportService.getUserShopReportDailyReportCount(user.userId)
        val checkReportAccess =
            userRestrictionService.checkShopReportAccess(user, userReportDailyReportCount?.toInt() ?: 0)
        if (checkReportAccess == UserRestrictionService.RestrictionResult.PROHIBIT) {
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).build()
        }

        // Save report job
        val jobId = UUID.randomUUID().toString()
        val report = reportRepository.findByRequestIdAndSellerLink(idempotenceKey, sellerLink).awaitSingleOrNull()
        if (report != null && report.status != ReportStatus.FAILED) {
            return ResponseEntity.ok().body(ReportJob(report.jobId, report.reportId))
        }
        reportRepository.save(
            ReportDocument(
                reportId = null,
                requestId = idempotenceKey,
                jobId = jobId,
                userId = user.userId,
                period = null,
                interval = daysPeriod,
                createdAt = LocalDateTime.now(),
                sellerLink = sellerLink,
                categoryPublicId = null,
                reportType = ReportType.SELLER,
                status = ReportStatus.PROCESSING,
                version = ReportVersion.V1
            )
        ).awaitSingleOrNull()

        return ResponseEntity.ok().body(ReportJob(jobId))
    }

    @GetMapping("/report/category")
    suspend fun generateReportByCategory(
        @RequestParam(name = "ids") ids: List<Long>,
        @RequestParam(name = "period") daysPeriod: Int,
        @RequestHeader(name = "X-API-KEY", required = true) apiKey: String,
        @RequestHeader(name = "Idempotence-Key", required = true) idempotenceKey: String
    ): ResponseEntity<Any> {
        val user = userRepository.findByApiKey_HashKey(apiKey).awaitSingleOrNull()
            ?: throw IllegalStateException("User not found")

        // Check report period permission
        val checkDaysAccess = userRestrictionService.checkDaysAccess(user, daysPeriod)
        if (checkDaysAccess == UserRestrictionService.RestrictionResult.PROHIBIT) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).build()
        }

        // Check daily report count permission
        val userReportDailyReportCount = reportService.getUserCategoryReportDailyReportCount(user.userId)
        val checkReportAccess = userRestrictionService.checkCategoryReportAccess(
            user, userReportDailyReportCount?.toInt() ?: 0
        )
        if (checkReportAccess == UserRestrictionService.RestrictionResult.PROHIBIT) {
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).build()
        }

        val categoryDocument = if (ids.size == 1) {
            categoryRepository.findByPublicId(ids.first()).awaitSingleOrNull()
                ?: throw IllegalArgumentException("Invalid category id")
        } else {
            val path = productService.buildCategoryPath(ids)
            categoryRepository.findByPath(path).awaitFirstOrNull()
                ?: throw IllegalArgumentException("Invalid category path: $path")
        }
        val jobId = UUID.randomUUID().toString()
        val report = reportRepository.findByRequestIdAndCategoryPublicId(idempotenceKey, categoryDocument.publicId)
            .awaitSingleOrNull()
        if (report != null && report.status != ReportStatus.FAILED) {
            return ResponseEntity.ok().body(ReportJob(report.jobId, report.reportId))
        }
        val categoryTitlePath = ids.map {
            categoryRepository.findByPublicId(it).awaitSingle().title
        }
        log.info { "Create report document with categoryId=${categoryDocument.publicId} and categoryPath=$categoryTitlePath" }
        reportRepository.save(
            ReportDocument(
                reportId = null,
                requestId = idempotenceKey,
                jobId = jobId,
                userId = user.userId,
                period = null,
                interval = daysPeriod,
                createdAt = LocalDateTime.now(),
                sellerLink = null,
                categoryPublicId = categoryDocument.publicId,
                categoryPath = categoryTitlePath,
                reportType = ReportType.CATEGORY,
                status = ReportStatus.PROCESSING,
                version = ReportVersion.V1
            )
        ).awaitSingleOrNull()

        return ResponseEntity.ok().body(ReportJob(jobId))
    }

    @GetMapping("/report/{report_id}")
    suspend fun getReportBySellerWithId(
        @PathVariable(name = "report_id") reportId: String,
    ): ResponseEntity<ByteArray> {
        val report = reportFileService.getReport(reportId) ?: return ResponseEntity.notFound().build()
        val reportData = withContext(Dispatchers.IO) {
            report.stream.readAllBytes()
        }

        return ResponseEntity.ok()
            .contentType(MediaType.APPLICATION_OCTET_STREAM)
            .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"${report.name}.xlsx\"")
            .body(reportData)
    }

    @GetMapping("/report/state/{job_id}")
    suspend fun getReportStatus(@PathVariable(name = "job_id") jobId: String): ResponseEntity<ReportStatusView> {
        val reportDoc = reportRepository.findByJobId(jobId).awaitSingleOrNull()
            ?: return ResponseEntity.notFound().build()
        return ResponseEntity.ok(
            ReportStatusView(
                reportId = reportDoc.reportId,
                jobId = reportDoc.jobId,
                status = reportDoc.status.name.lowercase(),
                interval = reportDoc.interval ?: -1,
                reportType = reportDoc.reportType?.name ?: "unknown",
                createdAt = reportDoc.createdAt,
                sellerLink = reportDoc.sellerLink,
                categoryId = reportDoc.categoryPublicId
            )
        )
    }

    @GetMapping("/reports")
    suspend fun getReports(
        @RequestHeader(name = "X-API-KEY", required = true) apiKey: String,
        @RequestParam(name = "from_time") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) fromTime: LocalDateTime,
    ): ResponseEntity<MutableList<ReportStatusView>> {
        val user = userRepository.findByApiKey_HashKey(apiKey).awaitSingleOrNull()
            ?: return ResponseEntity.notFound().build()
        val reportDocuments =
            reportRepository.findByUserIdAndCreatedAtFromTime(user.userId, fromTime).map { reportDoc ->
                ReportStatusView(
                    reportId = reportDoc.reportId,
                    jobId = reportDoc.jobId,
                    status = reportDoc.status.name.lowercase(),
                    interval = reportDoc.interval ?: -1,
                    reportType = reportDoc.reportType?.name ?: "unknown",
                    createdAt = reportDoc.createdAt,
                    sellerLink = reportDoc.sellerLink,
                    categoryId = reportDoc.categoryPublicId
                )
            }.collectList().awaitSingleOrNull() ?: return ResponseEntity.notFound().build()
        return ResponseEntity.ok(reportDocuments)
    }

    @GetMapping("/similar/products")
    suspend fun getSimilarProducts(
        @RequestParam(value = "productId", required = true) @Valid productId: Long,
        @RequestParam(value = "skuId", required = true) @Valid skuId: Long
    ): ResponseEntity<List<SimilarItemView>> {
        val url = "http://ke-space:8080/v1/similar/products?productId=$productId&skuId=$skuId"
        val httpHeaders = HttpHeaders().apply {
            set("Authorization", "Basic cHJvbWV0aGV1czptYXJrZXRsb2xwYXNzd2Q=")
            set("X-Request-Id", UUID.randomUUID().toString())
        }
        val httpEntity = HttpEntity<Any>(httpHeaders)
        val responseEntity = restTemplate.exchange<List<SimilarItemView>>(url, HttpMethod.GET, httpEntity)
        if (responseEntity.statusCode.is4xxClientError) {
            return ResponseEntity.badRequest().build()
        } else if (responseEntity.statusCode.is5xxServerError) {
            log.warn { "Bad similar item response: ${responseEntity.body}" }
            return ResponseEntity.internalServerError().build()
        }

        return ResponseEntity.ok(responseEntity.body!!)
    }

    @GetMapping("/category/{category_id}/product/{product_id}/sku/{sku_id}/positions")
    suspend fun getProductPositions(
        @PathVariable(name = "category_id") categoryId: Long,
        @PathVariable(name = "product_id") productId: Long,
        @PathVariable(name = "sku_id") skuId: Long,
        @RequestParam(name = "from_time") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) fromTime: LocalDateTime,
        @RequestParam(name = "to_time") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) toTime: LocalDateTime,
        @RequestHeader(name = "X-API-KEY", required = true) apiKey: String,
    ): ResponseEntity<ProductPositionView> {
        if (!checkRequestDaysPermission(apiKey, fromTime, toTime)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).build()
        }
        val productPositions = productService.getProductPositions(categoryId, productId, skuId, fromTime, toTime)
            ?: return ResponseEntity.ok(null)

        if (productPositions.isEmpty()) return ResponseEntity.ok(null)

        val positionAggregate = productPositions.first()

        return ResponseEntity.ok(ProductPositionView(
            categoryId = positionAggregate.id!!.categoryId,
            productId = positionAggregate.id!!.productId,
            skuId = positionAggregate.id!!.skuId,
            history = productPositions.map {
                ProductPositionHistoryView(
                    position = it.position!!,
                    date = it.id?.date!!
                )
            }
        ))

    }

    private suspend fun checkRequestDaysPermission(
        apiKey: String,
        fromTime: LocalDateTime,
        toTime: LocalDateTime
    ): Boolean {
        val daysCount = ChronoUnit.DAYS.between(fromTime, toTime)
        if (daysCount <= 0) return true
        val user = userRepository.findByApiKey_HashKey(apiKey).awaitSingleOrNull()
            ?: throw IllegalStateException("User not found")
        val checkDaysAccess = userRestrictionService.checkDaysAccess(user, daysCount.toInt())
        if (checkDaysAccess == UserRestrictionService.RestrictionResult.PROHIBIT) {
            return false
        }
        val checkDaysHistoryAccess = userRestrictionService.checkDaysHistoryAccess(user, fromTime)
        if (checkDaysHistoryAccess == UserRestrictionService.RestrictionResult.PROHIBIT) {
            return false
        }
        return true
    }

}
