package dev.crashteam.keanalytics.rpc

import dev.crashteam.keanalytics.db.model.enums.SubscriptionType
import dev.crashteam.keanalytics.repository.postgres.UserRepository
import dev.crashteam.keanalytics.service.UserSubscriptionService
import dev.crashteam.keanalytics.service.exception.UserSubscriptionGiveawayException
import dev.crashteam.mp.external.analytics.management.ExternalCategoryAnalyticsServiceGrpc
import dev.crashteam.mp.external.analytics.management.GiveSubscriptionRequest
import dev.crashteam.mp.external.analytics.management.GiveSubscriptionResponse
import io.grpc.stub.StreamObserver
import mu.KotlinLogging
import net.devh.boot.grpc.server.service.GrpcService

private val log = KotlinLogging.logger {}

@GrpcService
class ExternalAnalyticsManagementService(
    private val userSubscriptionService: UserSubscriptionService,
) :
    ExternalCategoryAnalyticsServiceGrpc.ExternalCategoryAnalyticsServiceImplBase() {

    override fun giveSubscription(
        request: GiveSubscriptionRequest,
        responseObserver: StreamObserver<GiveSubscriptionResponse>,
    ) {
        try {
            if (request.subscriptionId == "demo") {
                userSubscriptionService.giveawayDemoSubscription(request.userId, 3)
            } else {
                val subscriptionType = when (request.subscriptionId) {
                    "default" -> SubscriptionType.default_
                    "advanced" -> SubscriptionType.advanced
                    "pro" -> SubscriptionType.pro
                    else -> SubscriptionType.default_
                }
                userSubscriptionService.setUserSubscription(request.userId, subscriptionType, request.dayCount.toLong())
            }
        } catch (e: UserSubscriptionGiveawayException) {
            responseObserver.onNext(GiveSubscriptionResponse.newBuilder().apply {
                this.errorResponse = GiveSubscriptionResponse.ErrorResponse.newBuilder().apply {
                    this.errorCode = GiveSubscriptionResponse.ErrorResponse.ErrorCode.ERROR_CODE_SUBSCRIPTION_CONFLICT
                }.build()
            }.build())
        }
    }
}
