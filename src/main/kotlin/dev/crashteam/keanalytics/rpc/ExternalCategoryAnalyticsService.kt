package dev.crashteam.keanalytics.rpc

import dev.crashteam.keanalytics.extensions.toLocalDate
import dev.crashteam.keanalytics.extensions.toRepositoryDomain
import dev.crashteam.keanalytics.repository.clickhouse.model.SortBy
import dev.crashteam.keanalytics.repository.clickhouse.model.SortField
import dev.crashteam.keanalytics.service.CategoryAnalyticsService
import dev.crashteam.mp.external.analytics.category.*
import io.grpc.stub.StreamObserver
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import net.devh.boot.grpc.server.service.GrpcService
import org.springframework.core.convert.ConversionService

private val log = KotlinLogging.logger {}

@GrpcService
class ExternalCategoryAnalyticsService(
    private val categoryAnalyticsService: CategoryAnalyticsService,
    private val conversionService: ConversionService,
) : ExternalCategoryAnalyticsServiceGrpc.ExternalCategoryAnalyticsServiceImplBase() {

    override fun getCategoryAnalytics(
        request: GetCategoryAnalyticsRequest,
        responseObserver: StreamObserver<GetCategoryAnalyticsResponse>
    ) {
        try {
            log.debug { "Request getCategoryAnalytics: $request" }
            val categoriesAnalytics = if (request.hasCategoryId()) {
                runBlocking {
                    categoryAnalyticsService.getCategoryAnalytics(
                        categoryId = request.categoryId,
                        fromTime = request.dateRange.fromDate.toLocalDate(),
                        toTime = request.dateRange.toDate.toLocalDate(),
                        sortBy = if (request.sortList.isNotEmpty()) {
                            SortBy(
                                sortFields = request.sortList.map {
                                    SortField(
                                        fieldName = it.fieldName,
                                        order = it.order.toRepositoryDomain()
                                    )
                                }
                            )
                        } else null
                    )
                }
            } else {
                runBlocking {
                    categoryAnalyticsService.getRootCategoryAnalytics(
                        fromTime = request.dateRange.fromDate.toLocalDate(),
                        toTime = request.dateRange.toDate.toLocalDate(),
                        sortBy = if (request.sortList.isNotEmpty()) {
                            SortBy(
                                sortFields = request.sortList.map {
                                    SortField(
                                        fieldName = it.fieldName,
                                        order = it.order.toRepositoryDomain()
                                    )
                                }
                            )
                        } else null
                    )
                }
            }
            if (categoriesAnalytics.isNullOrEmpty()) {
                log.debug { "Failed get category analytics. Empty categoryAnalytics response" }
                responseObserver.onNext(GetCategoryAnalyticsResponse.newBuilder().apply {
                    this.errorResponse = GetCategoryAnalyticsResponse.ErrorResponse.newBuilder().apply {
                        this.errorCode = GetCategoryAnalyticsResponse.ErrorResponse.ErrorCode.ERROR_CODE_NOT_FOUND
                    }.build()
                }.build())
            } else {
                responseObserver.onNext(GetCategoryAnalyticsResponse.newBuilder().apply {
                    this.successResponse = GetCategoryAnalyticsResponse.SuccessResponse.newBuilder().apply {
                        this.addAllCategories(categoriesAnalytics.map { categoryAnalyticsInfo ->
                            conversionService.convert(
                                categoryAnalyticsInfo,
                                dev.crashteam.mp.external.analytics.category.CategoryAnalyticsInfo::class.java
                            )
                        })
                    }.build()
                    log.debug { "Response getCategoriesAnalytics: ${this.successResponse}" }
                }.build())
            }
            responseObserver.onCompleted()
        } catch (e: Exception) {
            log.error(e) { "Exception during get category analytics" }
            responseObserver.onNext(GetCategoryAnalyticsResponse.newBuilder().apply {
                this.errorResponse = GetCategoryAnalyticsResponse.ErrorResponse.newBuilder().apply {
                    this.errorCode = GetCategoryAnalyticsResponse.ErrorResponse.ErrorCode.ERROR_CODE_UNEXPECTED
                }.build()
            }.build())
            responseObserver.onCompleted()
        }
    }

    override fun getCategoryDailyAnalytics(
        request: GetCategoryDailyAnalyticsRequest,
        responseObserver: StreamObserver<GetCategoryDailyAnalyticsResponse>
    ) {
        try {
            log.debug { "Request getCategoryDailyAnalytics: $request" }
            val categoryDailyAnalytics = categoryAnalyticsService.getCategoryDailyAnalytics(
                categoryId = request.categoryId,
                fromTime = request.dateRange.fromDate.toLocalDate(),
                toTime = request.dateRange.toDate.toLocalDate(),
            )
            log.debug { "Category daily analytics: $categoryDailyAnalytics" }
            responseObserver.onNext(GetCategoryDailyAnalyticsResponse.newBuilder().apply {
                this.successResponse = GetCategoryDailyAnalyticsResponse.SuccessResponse.newBuilder().apply {
                    this.addAllCategoryDailyAnalyticsInfo(categoryDailyAnalytics.map { categoryDailyAnalytics ->
                        conversionService.convert(categoryDailyAnalytics, CategoryDailyAnalyticsInfo::class.java)
                    }).build()
                }.build()
                log.debug { "Response getCategoryDailyAnalytics: ${this.successResponse}" }
            }.build())
            responseObserver.onCompleted()
        } catch (e: Exception) {
            log.error(e) { "Exception during get category daily analytics" }
            responseObserver.onNext(GetCategoryDailyAnalyticsResponse.newBuilder().apply {
                this.errorResponse = GetCategoryDailyAnalyticsResponse.ErrorResponse.newBuilder().apply {
                    this.errorCode = GetCategoryDailyAnalyticsResponse.ErrorResponse.ErrorCode.ERROR_CODE_UNEXPECTED
                }.build()
            }.build())
            responseObserver.onCompleted()
        }
    }

    override fun getCategoryAnalyticsProducts(
        request: GetCategoryAnalyticsProductsRequest,
        responseObserver: StreamObserver<GetCategoryAnalyticsProductResponse>
    ) {
        try {
            log.debug { "Request getCategoryAnalyticsProducts: $request" }
            val productsAnalytics = categoryAnalyticsService.getCategoryProductsAnalytics(
                categoryId = request.categoryId,
                datePeriod = request.datePeriod,
                filter = request.filterList,
                sort = request.sortList,
                page = request.pagination
            )
            responseObserver.onNext(GetCategoryAnalyticsProductResponse.newBuilder().apply {
                this.successResponse = GetCategoryAnalyticsProductResponse.SuccessResponse.newBuilder().apply {
                    this.categoryProductAnalytics = CategoryProductAnalytics.newBuilder().apply {
                        this.addAllProductAnalytics(productsAnalytics)
                    }.build()
                }.build()
                log.debug { "Response getCategoryAnalyticsProducts: ${this.successResponse}" }
            }.build())
            responseObserver.onCompleted()
        } catch (e: Exception) {
            log.error(e) { "Exception during get category products analytics" }
            responseObserver.onNext(GetCategoryAnalyticsProductResponse.newBuilder().apply {
                this.errorResponse = GetCategoryAnalyticsProductResponse.ErrorResponse.newBuilder().apply {
                    this.errorCode = GetCategoryAnalyticsProductResponse.ErrorResponse.ErrorCode.ERROR_CODE_UNEXPECTED
                }.build()
            }.build())
            responseObserver.onCompleted()
        }
    }

    override fun getProductDailyAnalytics(
        request: GetProductDailyAnalyticsRequest,
        responseObserver: StreamObserver<GetProductDailyAnalyticsResponse>
    ) {
        super.getProductDailyAnalytics(request, responseObserver)
    }
}
