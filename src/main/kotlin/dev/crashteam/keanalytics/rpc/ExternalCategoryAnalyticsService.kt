package dev.crashteam.keanalytics.rpc

import dev.crashteam.keanalytics.extensions.toLocalDate
import dev.crashteam.keanalytics.extensions.toLocalDates
import dev.crashteam.keanalytics.extensions.toRepositoryDomain
import dev.crashteam.keanalytics.repository.clickhouse.model.SortBy
import dev.crashteam.keanalytics.repository.clickhouse.model.SortField
import dev.crashteam.keanalytics.repository.mongo.UserRepository
import dev.crashteam.keanalytics.service.CategoryAnalyticsSortableDecorator
import dev.crashteam.keanalytics.service.ProductServiceAnalytics
import dev.crashteam.keanalytics.service.UserRestrictionService
import dev.crashteam.mp.base.DatePeriod
import dev.crashteam.mp.external.analytics.category.*
import io.grpc.stub.StreamObserver
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import net.devh.boot.grpc.server.service.GrpcService
import org.springframework.core.convert.ConversionService
import java.time.LocalDate
import java.time.temporal.ChronoUnit

private val log = KotlinLogging.logger {}

@GrpcService
class ExternalCategoryAnalyticsService(
    private val categoryAnalyticsService: CategoryAnalyticsSortableDecorator,
    private val productServiceAnalytics: ProductServiceAnalytics,
    private val conversionService: ConversionService,
    private val userRepository: UserRepository,
    private val userRestrictionService: UserRestrictionService,
) : ExternalCategoryAnalyticsServiceGrpc.ExternalCategoryAnalyticsServiceImplBase() {

    override fun getCategoryAnalytics(
        request: GetCategoryAnalyticsRequest,
        responseObserver: StreamObserver<GetCategoryAnalyticsResponse>
    ) {
        try {
            log.debug { "Request getCategoryAnalytics: $request" }
            if (!checkRequestDaysPermission(request.userId, request.datePeriod)) {
                responseObserver.onNext(GetCategoryAnalyticsResponse.newBuilder().apply {
                    this.errorResponse = GetCategoryAnalyticsResponse.ErrorResponse.newBuilder().apply {
                        this.errorCode = GetCategoryAnalyticsResponse.ErrorResponse.ErrorCode.ERROR_CODE_FORBIDDEN
                    }.build()
                }.build())
                return
            }
            val categoriesAnalytics = runBlocking {
                try {
                    if (request.hasCategoryId()) {
                        categoryAnalyticsService.getCategoryAnalytics(
                            categoryId = request.categoryId,
                            datePeriod = request.datePeriod,
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
                    } else {
                        categoryAnalyticsService.getRootCategoryAnalytics(
                            datePeriod = request.datePeriod,
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
                } catch (e: Exception) {
                    log.error(e) { "Exception during get categories. request=$request" }
                    throw e
                }
            }
            if (categoriesAnalytics.isEmpty()) {
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
        } catch (e: Exception) {
            log.error(e) { "Exception during get category analytics" }
            responseObserver.onNext(GetCategoryAnalyticsResponse.newBuilder().apply {
                this.errorResponse = GetCategoryAnalyticsResponse.ErrorResponse.newBuilder().apply {
                    this.errorCode = GetCategoryAnalyticsResponse.ErrorResponse.ErrorCode.ERROR_CODE_UNEXPECTED
                    this.description = e.message
                }.build()
            }.build())
        } finally {
            responseObserver.onCompleted()
        }
    }

    override fun getCategoryDailyAnalytics(
        request: GetCategoryDailyAnalyticsRequest,
        responseObserver: StreamObserver<GetCategoryDailyAnalyticsResponse>
    ) {
        try {
            log.debug { "Request getCategoryDailyAnalytics: $request" }
            val fromDate = request.dateRange.fromDate.toLocalDate()
            val toDate = request.dateRange.toDate.toLocalDate()
            if (!checkRequestDaysPermission(request.userId, fromDate, toDate)) {
                responseObserver.onNext(GetCategoryDailyAnalyticsResponse.newBuilder().apply {
                    this.errorResponse = GetCategoryDailyAnalyticsResponse.ErrorResponse.newBuilder().apply {
                        this.errorCode = GetCategoryDailyAnalyticsResponse.ErrorResponse.ErrorCode.ERROR_CODE_FORBIDDEN
                    }.build()
                }.build())
                return
            }
            val categoryDailyAnalytics = categoryAnalyticsService.getCategoryDailyAnalytics(
                categoryId = request.categoryId,
                fromTime = fromDate,
                toTime = toDate,
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
        } catch (e: Exception) {
            log.error(e) { "Exception during get category daily analytics" }
            responseObserver.onNext(GetCategoryDailyAnalyticsResponse.newBuilder().apply {
                this.errorResponse = GetCategoryDailyAnalyticsResponse.ErrorResponse.newBuilder().apply {
                    this.errorCode = GetCategoryDailyAnalyticsResponse.ErrorResponse.ErrorCode.ERROR_CODE_UNEXPECTED
                }.build()
            }.build())
        } finally {
            responseObserver.onCompleted()
        }
    }

    override fun getCategoryAnalyticsProducts(
        request: GetCategoryAnalyticsProductsRequest,
        responseObserver: StreamObserver<GetCategoryAnalyticsProductResponse>
    ) {
        try {
            log.debug { "Request getCategoryAnalyticsProducts: $request" }
            if (!checkRequestDaysPermission(request.userId, request.datePeriod)) {
                responseObserver.onNext(GetCategoryAnalyticsProductResponse.newBuilder().apply {
                    this.errorResponse = GetCategoryAnalyticsProductResponse.ErrorResponse.newBuilder().apply {
                        this.errorCode =
                            GetCategoryAnalyticsProductResponse.ErrorResponse.ErrorCode.ERROR_CODE_FORBIDDEN
                    }.build()
                }.build())
                return
            }
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
        } catch (e: Exception) {
            log.error(e) { "Exception during get category products analytics" }
            responseObserver.onNext(GetCategoryAnalyticsProductResponse.newBuilder().apply {
                this.errorResponse = GetCategoryAnalyticsProductResponse.ErrorResponse.newBuilder().apply {
                    this.errorCode = GetCategoryAnalyticsProductResponse.ErrorResponse.ErrorCode.ERROR_CODE_UNEXPECTED
                }.build()
            }.build())
        } finally {
            responseObserver.onCompleted()
        }
    }

    override fun getProductDailyAnalytics(
        request: GetProductDailyAnalyticsRequest,
        responseObserver: StreamObserver<GetProductDailyAnalyticsResponse>
    ) {
        try {
            log.debug { "Request getProductDailyAnalytics: $request" }
            val fromDate = request.dateRange.fromDate.toLocalDate()
            val toDate = request.dateRange.toDate.toLocalDate()
            if (!checkRequestDaysPermission(request.userId, fromDate, toDate)) {
                responseObserver.onNext(GetProductDailyAnalyticsResponse.newBuilder().apply {
                    this.errorResponse = GetProductDailyAnalyticsResponse.ErrorResponse.newBuilder().apply {
                        this.errorCode = GetProductDailyAnalyticsResponse.ErrorResponse.ErrorCode.ERROR_CODE_FORBIDDEN
                    }.build()
                }.build())
                return
            }
            val productDailyAnalytics = productServiceAnalytics.getProductDailyAnalytics(
                request.productId.toString(),
                fromDate,
                toDate
            )
            if (productDailyAnalytics == null) {
                responseObserver.onNext(GetProductDailyAnalyticsResponse.newBuilder().apply {
                    this.errorResponse = GetProductDailyAnalyticsResponse.ErrorResponse.newBuilder().apply {
                        this.errorCode = GetProductDailyAnalyticsResponse.ErrorResponse.ErrorCode.ERROR_CODE_NOT_FOUND
                    }.build()
                }.build())
            } else {
                responseObserver.onNext(GetProductDailyAnalyticsResponse.newBuilder().apply {
                    this.successResponse = GetProductDailyAnalyticsResponse.SuccessResponse.newBuilder().apply {
                        this.productDailyAnalytics =
                            conversionService.convert(productDailyAnalytics, ProductDailyAnalytics::class.java)
                    }.build()
                    log.debug { "Response getCategoryAnalyticsProducts: ${this.successResponse}" }
                }.build())
            }
        } catch (e: Exception) {
            log.error(e) { "Exception during get product daily analytics" }
            responseObserver.onNext(GetProductDailyAnalyticsResponse.newBuilder().apply {
                this.errorResponse = GetProductDailyAnalyticsResponse.ErrorResponse.newBuilder().apply {
                    this.errorCode = GetProductDailyAnalyticsResponse.ErrorResponse.ErrorCode.ERROR_CODE_UNEXPECTED
                }.build()
            }.build())
        } finally {
            responseObserver.onCompleted()
        }
    }

    override fun getCategory(
        request: GetCategoryDataRequest,
        responseObserver: StreamObserver<GetCategoryDataResponse>
    ) {
        try {
            log.debug { "Request getCategory: $request" }
            val categoryInfo = categoryAnalyticsService.getCategoryInfo(request.categoryId.toLong())
            if (categoryInfo == null) {
                responseObserver.onNext(GetCategoryDataResponse.newBuilder().apply {
                    this.errorResponse = GetCategoryDataResponse.ErrorResponse.newBuilder().apply {
                        this.errorCode = GetCategoryDataResponse.ErrorResponse.ErrorCode.ERROR_CODE_NOT_FOUND
                    }.build()
                }.build())
            } else {
                responseObserver.onNext(GetCategoryDataResponse.newBuilder().apply {
                    this.successResponse = GetCategoryDataResponse.SuccessResponse.newBuilder().apply {
                        this.categoryInfo = CategoryInfo.newBuilder().apply {
                            this.categoryId = request.categoryId
                            this.name = categoryInfo.name
                            this.parentId = categoryInfo.parentId.toString()
                            this.addAllChildrenIds(categoryInfo.childrenIds.map { it.toString() })
                        }.build()
                    }.build()
                    log.debug { "Response getCategory: ${this.successResponse}" }
                }.build())
            }
        } catch (e: Exception) {
            log.error(e) { "Exception during get category info" }
            responseObserver.onNext(GetCategoryDataResponse.newBuilder().apply {
                this.errorResponse = GetCategoryDataResponse.ErrorResponse.newBuilder().apply {
                    this.errorCode = GetCategoryDataResponse.ErrorResponse.ErrorCode.ERROR_CODE_UNEXPECTED
                }.build()
            }.build())
        } finally {
            responseObserver.onCompleted()
        }
    }

    private fun checkRequestDaysPermission(
        userId: String,
        datePeriod: DatePeriod,
    ): Boolean {
        val localDatesPeriod = datePeriod.toLocalDates()
        return checkRequestDaysPermission(userId, localDatesPeriod.fromDate, localDatesPeriod.toDate)
    }

    private fun checkRequestDaysPermission(
        userId: String,
        fromDate: LocalDate,
        toDate: LocalDate
    ): Boolean {
        log.debug { "Check request days permission. userId=$userId; fromDate=$fromDate; toDate=$toDate" }
        val daysCount = ChronoUnit.DAYS.between(fromDate, toDate)
        if (daysCount <= 0) return true
        val user = userRepository.findByUserId(userId).block()
            ?: throw IllegalStateException("User not found")
        val checkDaysAccess = userRestrictionService.checkDaysAccess(user, daysCount.toInt())
        if (checkDaysAccess == UserRestrictionService.RestrictionResult.PROHIBIT) {
            log.debug { "Check request days, permission prohibited. userId=$userId; requestDaysCount=$daysCount" }
            return false
        }
        val checkDaysHistoryAccess = userRestrictionService.checkDaysHistoryAccess(user, fromDate.atStartOfDay())
        if (checkDaysHistoryAccess == UserRestrictionService.RestrictionResult.PROHIBIT) {
            log.debug { "Check request days history, permission prohibited. userId=$userId; requestDaysCount=$daysCount" }
            return false
        }
        return true
    }
}
