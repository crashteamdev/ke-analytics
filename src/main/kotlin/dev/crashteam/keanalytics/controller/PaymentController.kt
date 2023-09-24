package dev.crashteam.keanalytics.controller

import dev.crashteam.keanalytics.calculator.PriceCalculatorContext
import dev.crashteam.keanalytics.calculator.PriceCalculatorFactory
import dev.crashteam.keanalytics.calculator.PriceCalculatorOption
import dev.crashteam.keanalytics.client.freekassa.FreeKassaClient
import dev.crashteam.keanalytics.client.freekassa.model.PaymentFormRequestParams
import dev.crashteam.keanalytics.config.properties.FreeKassaProperties
import dev.crashteam.keanalytics.controller.model.PaymentCreate
import dev.crashteam.keanalytics.controller.model.PaymentCreateResponse
import dev.crashteam.keanalytics.controller.model.PaymentSubscriptionUpgradeCreate
import dev.crashteam.keanalytics.domain.mongo.PromoCodeType
import dev.crashteam.keanalytics.extensions.mapToSubscription
import dev.crashteam.keanalytics.repository.mongo.PromoCodeRepository
import dev.crashteam.keanalytics.repository.mongo.ReferralCodeRepository
import dev.crashteam.keanalytics.repository.mongo.UserRepository
import dev.crashteam.keanalytics.service.PaymentService
import dev.crashteam.keanalytics.service.UserService
import dev.crashteam.keanalytics.service.model.CallbackPaymentAdditionalInfo
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import mu.KotlinLogging
import org.apache.commons.codec.digest.DigestUtils
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import org.springframework.web.server.ServerWebExchange
import java.math.BigDecimal
import java.security.Principal
import java.time.LocalDateTime
import java.util.*

private val log = KotlinLogging.logger {}

@RestController
@RequestMapping(path = ["v1"], produces = [MediaType.APPLICATION_JSON_VALUE])
class PaymentController(
    private val freeKassaProperties: FreeKassaProperties,
    private val freeKassaClient: FreeKassaClient,
    private val paymentService: PaymentService,
    private val userService: UserService,
    private val priceCalculatorFactory: PriceCalculatorFactory,
    private val promoCodeRepository: PromoCodeRepository,
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
        val priceCalculator =
            priceCalculatorFactory.createPriceCalculator(
                PriceCalculatorOption(
                    body.promoCode,
                    body.referralCode,
                    context = PriceCalculatorContext(principal.name, body.multiply ?: 1, userSubscription)
                )
            )
        val price = priceCalculator.calculatePrice()
        val paymentId = UUID.randomUUID().toString()
        val promoCodeDocument = if (body.promoCode != null) {
            promoCodeRepository.findByCode(body.promoCode).awaitSingleOrNull()
        } else null
        val freekassaPaymentRequest = PaymentFormRequestParams(
            userId = principal.name,
            orderId = paymentId,
            email = body.email!!,
            amount = price,
            currency = "RUB",
            subscriptionId = userSubscription.num,
            referralCode = body.referralCode,
            promoCode = body.promoCode,
            promoCodeType = promoCodeDocument?.type,
            multiply = body.multiply ?: 1
        )
        val paymentUrl = freeKassaClient.createPaymentFormUrl(freekassaPaymentRequest)

        val payment = paymentService.findPayment(paymentId)
        if (payment == null) {
            paymentService.savePayment(
                paymentId,
                principal.name,
                "pending",
                false,
                price.toString(),
                userSubscription.num,
                body.multiply ?: 1,
                referralCode = body.referralCode
            ).awaitSingleOrNull()
        }

        return ResponseEntity.ok().body(
            PaymentCreateResponse(
                paymentId = paymentId,
                status = "pending",
                paid = false,
                amount = price.toString(),
                currency = "RUB",
                confirmationUrl = paymentUrl
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

    @PostMapping(
        "/payment/fk/callback",
        consumes = [MediaType.APPLICATION_FORM_URLENCODED_VALUE, MediaType.MULTIPART_FORM_DATA_VALUE]
    )
    suspend fun callbackPayment(
        exchange: ServerWebExchange,
    ): ResponseEntity<String> {
        val formData = exchange.formData.awaitSingle()
        val merchantId = formData["MERCHANT_ID"]?.singleOrNull()
        val amount = formData["AMOUNT"]?.singleOrNull()
        val orderId = formData["MERCHANT_ORDER_ID"]?.singleOrNull()
        val paymentId = formData["us_paymentid"]?.singleOrNull()
        val curId = formData["CUR_ID"]?.singleOrNull()
        val userId = formData["us_userid"]?.singleOrNull()
        val subscriptionId = formData["us_subscriptionid"]?.singleOrNull()
        val promoCode = formData["us_promocode"]?.singleOrNull()
        val promoCodeType = formData["us_promocodeType"]?.singleOrNull()
        log.info { "Callback freekassa payment. Body=$formData" }
        if (merchantId == null || amount == null || orderId == null || curId == null
            || userId == null || subscriptionId == null || paymentId == null
        ) {
            log.warn { "Callback payment. Bad request. Body=$formData" }
            return ResponseEntity.badRequest().build()
        }
        val md5Hash =
            DigestUtils.md5Hex("$merchantId:$amount:${freeKassaProperties.secretWordSecond}:$orderId")
        if (formData["SIGN"]?.single() != md5Hash) {
            log.warn { "Callback payment sign is not valid. expected=$md5Hash; actual=${formData["SIGN"]?.single()}" }
            return ResponseEntity.badRequest().build()
        }
        val paymentAdditionalInfo = if (promoCode != null && promoCodeType != null) {
            CallbackPaymentAdditionalInfo(promoCode, PromoCodeType.valueOf(promoCodeType))
        } else {
            null
        }
        log.debug { "Callback payment additional info: $paymentAdditionalInfo" }
        paymentService.callbackPayment(
            paymentId,
            userId,
            curId,
            paymentAdditionalInfo = paymentAdditionalInfo
        )

        return ResponseEntity.ok("YES")
    }

}
