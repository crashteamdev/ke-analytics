package dev.crashteam.keanalytics.controller

import kotlinx.coroutines.reactor.awaitSingleOrNull
import mu.KotlinLogging
import dev.crashteam.keanalytics.client.yookassa.YooKassaClient
import dev.crashteam.keanalytics.client.yookassa.model.PaymentAmount
import dev.crashteam.keanalytics.client.yookassa.model.PaymentConfirmation
import dev.crashteam.keanalytics.client.yookassa.model.PaymentRequest
import dev.crashteam.keanalytics.controller.model.PaymentCreate
import dev.crashteam.keanalytics.controller.model.PaymentCreateResponse
import dev.crashteam.keanalytics.controller.model.PaymentSubscriptionUpgradeCreate
import dev.crashteam.keanalytics.domain.mongo.AdvancedSubscription
import dev.crashteam.keanalytics.domain.mongo.ProSubscription
import dev.crashteam.keanalytics.extensions.mapToSubscription
import dev.crashteam.keanalytics.repository.mongo.ReferralCodeRepository
import dev.crashteam.keanalytics.repository.mongo.UserRepository
import dev.crashteam.keanalytics.service.PaymentService
import dev.crashteam.keanalytics.service.UserService
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import java.math.BigDecimal
import java.security.Principal
import java.time.LocalDateTime

private val log = KotlinLogging.logger {}

@RestController
@RequestMapping(path = ["v1"], produces = [MediaType.APPLICATION_JSON_VALUE])
class PaymentController(
    val youKassaClient: YooKassaClient,
    val paymentService: PaymentService,
    val userRepository: UserRepository,
    val referralCodeRepository: ReferralCodeRepository,
    val userService: UserService
) {

    @PostMapping("/payment")
    suspend fun createPayment(
        @RequestBody body: PaymentCreate,
        @RequestHeader(name = "Idempotence-Key", required = true) idempotenceKey: String,
        principal: Principal,
    ): ResponseEntity<PaymentCreateResponse> {
        log.info { "Create payment $body. User=$principal; ReferralCode=${body.referralCode}" }
        val userSubscription = body.subscriptionType.mapToSubscription() ?: return ResponseEntity.badRequest().build()
        if (BigDecimal(body.amount) != BigDecimal(userSubscription.price()).setScale(2)) {
            return ResponseEntity.badRequest().build()
        }
        val userDocument = userRepository.findByUserId(principal.name).awaitSingleOrNull()
        val isUserCanUseReferral = if (body.referralCode != null && body.referralCode.isNotBlank()) {
            val userReferralCode = referralCodeRepository.findByUserId(principal.name).awaitSingleOrNull()
            val inviteReferralCode = referralCodeRepository.findByCode(body.referralCode).awaitSingleOrNull()
            log.info { "User referral code vs request code. ${userReferralCode?.code}=${body.referralCode}" }
            inviteReferralCode != null &&
                    userReferralCode?.code != inviteReferralCode.code &&
                    userDocument?.subscription == null
        } else false
        log.info { "IsUserCanUseReferral code: $isUserCanUseReferral" }
        val amount = if (body.multiply != null && body.multiply > 1) {
            val multipliedAmount = BigDecimal(body.amount) * BigDecimal.valueOf(body.multiply.toLong())
            val discount = if (body.multiply == 3.toShort()) {
                when (userSubscription) {
                    AdvancedSubscription -> {
                        BigDecimal(0.15)
                    }
                    ProSubscription -> {
                        BigDecimal(0.20)
                    }
                    else -> BigDecimal(0.10)
                }
            } else if (body.multiply >= 6) {
                BigDecimal(0.30)
            } else BigDecimal(0.10)
            (multipliedAmount - (multipliedAmount * discount)).toLong().toString()
        } else if (body.referralCode != null && body.referralCode.isNotBlank()) {
            if (isUserCanUseReferral) {
                (BigDecimal(body.amount) - (BigDecimal(body.amount) * BigDecimal(0.15))).toLong().toString()
            } else body.amount
        } else {
            body.amount
        }
        val paymentRequest = PaymentRequest(
            amount = PaymentAmount(amount, body.currency),
            capture = true,
            confirmation = PaymentConfirmation("redirect", body.redirectUrl),
            createdAt = LocalDateTime.now(),
            description = body.description,
            metadata = mapOf("sub_type" to userSubscription.name)
        )
        val paymentResponse = youKassaClient.createPayment(idempotenceKey, paymentRequest)

        val payment = paymentService.findPayment(paymentResponse.id)
        if (payment == null) {
            paymentService.savePayment(
                paymentResponse.id,
                principal.name,
                paymentResponse.status,
                paymentResponse.paid,
                paymentResponse.amount.value,
                userSubscription.num,
                body.multiply ?: 1,
                referralCode = body.referralCode
            ).awaitSingleOrNull()
        }

        return ResponseEntity.ok().body(
            PaymentCreateResponse(
                paymentId = paymentResponse.id,
                status = paymentResponse.status,
                paid = paymentResponse.paid,
                amount = paymentResponse.amount.value,
                currency = paymentResponse.amount.currency,
                confirmationUrl = paymentResponse.confirmation.confirmationUrl
            )
        )
    }

    @PostMapping("/payment/upgrade")
    suspend fun upgradeSubscription(
        @RequestBody body: PaymentSubscriptionUpgradeCreate,
        @RequestHeader(name = "Idempotence-Key", required = true) idempotenceKey: String,
        principal: Principal
    ): ResponseEntity<PaymentCreateResponse> {
        log.info { "Upgrade subscription. User=${principal.name}" }
        val user = userService.findUser(principal.name) ?: return ResponseEntity.notFound().build()
        val currentUserSubscription = user.subscription
        if (currentUserSubscription == null || currentUserSubscription.endAt <= LocalDateTime.now()) {
            return ResponseEntity.badRequest().build()
        }
        val userSubType = currentUserSubscription.subType?.mapToSubscription()!!
        if (userSubType.num > body.subscriptionType) {
            return ResponseEntity.badRequest().build()
        }
        val paymentResponse = paymentService.createUpgradeUserSubscriptonPayment(
            user,
            body.subscriptionType.mapToSubscription()!!,
            body.redirectUrl
        )

        return ResponseEntity.ok().body(
            PaymentCreateResponse(
                paymentId = paymentResponse.id,
                status = paymentResponse.status,
                paid = paymentResponse.paid,
                amount = paymentResponse.amount.value,
                currency = paymentResponse.amount.currency,
                confirmationUrl = paymentResponse.confirmation.confirmationUrl
            )
        )
    }
}
