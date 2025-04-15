package dev.crashteam.keanalytics.rpc

import dev.crashteam.keanalytics.db.model.enums.SubscriptionType
import dev.crashteam.keanalytics.service.UserSubscriptionService
import dev.crashteam.keanalytics.service.exception.UserSubscriptionGiveawayException
import dev.crashteam.mp.external.analytics.management.ExternalCategoryAnalyticsServiceGrpc
import dev.crashteam.mp.external.analytics.management.GiveSubscriptionRequest
import dev.crashteam.mp.external.analytics.management.GiveSubscriptionResponse
import io.grpc.stub.StreamObserver
import mu.KotlinLogging
import net.devh.boot.grpc.server.service.GrpcService
import org.springframework.transaction.annotation.Transactional

private val log = KotlinLogging.logger {}

@GrpcService
class ExternalAnalyticsManagementService(
    private val userSubscriptionService: UserSubscriptionService,
) :
    ExternalCategoryAnalyticsServiceGrpc.ExternalCategoryAnalyticsServiceImplBase() {

    @Transactional
    override fun giveSubscription(
        request: GiveSubscriptionRequest,
        responseObserver: StreamObserver<GiveSubscriptionResponse>,
    ) {
        try {
            if (request.userId.isBlank()) {
                sendError(responseObserver, GiveSubscriptionResponse.ErrorResponse.ErrorCode.ERROR_CODE_USER_NOT_FOUND)
                return
            }

            if (request.dayCount <= 0) {
                sendError(responseObserver, GiveSubscriptionResponse.ErrorResponse.ErrorCode.ERROR_CODE_BAD_REQUEST)
                return
            }

            if (request.subscriptionId == "demo") {
                userSubscriptionService.giveawayDemoSubscription(request.userId, request.dayCount.toLong())
            } else {
                val subscriptionType = when (request.subscriptionId) {
                    "default" -> SubscriptionType.default_
                    "advanced" -> SubscriptionType.advanced
                    "pro" -> SubscriptionType.pro
                    else -> {
                        sendError(
                            responseObserver,
                            GiveSubscriptionResponse.ErrorResponse.ErrorCode.ERROR_CODE_BAD_REQUEST
                        )
                        return
                    }
                }
                userSubscriptionService.setUserSubscription(request.userId, subscriptionType, request.dayCount.toLong())
            }

            responseObserver.onNext(GiveSubscriptionResponse.newBuilder().apply {
                this.setSuccessResponse(GiveSubscriptionResponse.SuccessResponse.newBuilder().build())
            }.build())
        } catch (e: UserSubscriptionGiveawayException) {
            sendError(
                responseObserver,
                GiveSubscriptionResponse.ErrorResponse.ErrorCode.ERROR_CODE_SUBSCRIPTION_CONFLICT
            )
        } catch (e: Exception) {
            log.error(e) { "Error while giving subscription to user ${request.userId}" }
            sendError(responseObserver, GiveSubscriptionResponse.ErrorResponse.ErrorCode.ERROR_CODE_UNEXPECTED)
        } finally {
            responseObserver.onCompleted()
        }
    }

    private fun sendError(
        responseObserver: StreamObserver<GiveSubscriptionResponse>,
        errorCode: GiveSubscriptionResponse.ErrorResponse.ErrorCode,
    ) {
        responseObserver.onNext(GiveSubscriptionResponse.newBuilder().apply {
            this.errorResponse = GiveSubscriptionResponse.ErrorResponse.newBuilder().apply {
                this.errorCode = errorCode
            }.build()
        }.build())
        responseObserver.onCompleted()
    }
}
