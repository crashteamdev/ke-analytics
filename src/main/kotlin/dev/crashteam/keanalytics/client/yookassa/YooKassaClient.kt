package dev.crashteam.keanalytics.client.yookassa

import com.fasterxml.jackson.databind.ObjectMapper
import dev.crashteam.keanalytics.config.properties.ServiceProperties
import dev.crashteam.keanalytics.config.properties.YouKassaProperties
import mu.KotlinLogging
import dev.crashteam.keanalytics.client.kazanexpress.model.proxy.ProxyRequestBody
import dev.crashteam.keanalytics.client.kazanexpress.model.proxy.ProxyRequestContext
import dev.crashteam.keanalytics.client.kazanexpress.model.proxy.StyxResponse
import dev.crashteam.keanalytics.client.yookassa.model.PaymentRequest
import dev.crashteam.keanalytics.client.yookassa.model.PaymentResponse
import dev.crashteam.keanalytics.client.yookassa.model.PaymentStatusResponse
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.HttpEntity
import org.springframework.http.HttpMethod
import org.springframework.http.HttpStatus.Series
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.stereotype.Component
import org.springframework.web.client.RestTemplate
import java.nio.charset.Charset
import java.util.Base64

private val log = KotlinLogging.logger {}

@Component
class YooKassaClient(
    private val restTemplate: RestTemplate,
    private val objectMapper: ObjectMapper,
    private val youKassaProperties: YouKassaProperties,
    private val serviceProperties: ServiceProperties
) {

    fun createPayment(idempotencyKey: String, paymentRequest: PaymentRequest): PaymentResponse {
        val json = objectMapper.writeValueAsString(paymentRequest)
        val auth = "${youKassaProperties.shopId}:${youKassaProperties.key}"
        val encodedAuth = Base64.getEncoder().encodeToString(
            auth.toByteArray(Charset.forName("US-ASCII"))
        )
        val authHeader = "Basic $encodedAuth"
        val proxyRequestBody = ProxyRequestBody(
            url = PAYMENTS_URL,
            httpMethod = "POST",
            context = listOf(
                ProxyRequestContext(
                    key = "headers",
                    value = mapOf(
                        "Authorization" to authHeader,
                        "Idempotence-Key" to idempotencyKey,
                        "Content-Type" to MediaType.APPLICATION_JSON_VALUE
                    )
                ),
                ProxyRequestContext("content", Base64.getEncoder().encodeToString(json.toByteArray()))
            )
        )
        val responseType: ParameterizedTypeReference<StyxResponse<PaymentResponse>> =
            object : ParameterizedTypeReference<StyxResponse<PaymentResponse>>() {}
        val styxResponse = restTemplate.exchange(
            "${serviceProperties.proxy!!.url}/v2/proxy",
            HttpMethod.POST,
            HttpEntity<ProxyRequestBody>(proxyRequestBody),
            responseType
        ).body

        return styxResponse?.body!!
    }

    fun checkStatus(paymentId: String): PaymentStatusResponse {
        val auth = "${youKassaProperties.shopId}:${youKassaProperties.key}"
        val encodedAuth = Base64.getEncoder().encodeToString(
            auth.toByteArray(Charset.forName("US-ASCII"))
        )
        val authHeader = "Basic $encodedAuth"
        val proxyRequestBody = ProxyRequestBody(
            url = PAYMENT_STATUS_URL + paymentId,
            httpMethod = "GET",
            context = listOf(
                ProxyRequestContext(
                    key = "headers",
                    value = mapOf(
                        "Authorization" to authHeader,
                        "Accept" to MediaType.APPLICATION_JSON_VALUE
                    )
                )
            )
        )
        val responseType: ParameterizedTypeReference<StyxResponse<PaymentStatusResponse>> =
            object : ParameterizedTypeReference<StyxResponse<PaymentStatusResponse>>() {}
        val styxResponse = restTemplate.exchange(
            "${serviceProperties.proxy!!.url}/v2/proxy",
            HttpMethod.POST,
            HttpEntity<ProxyRequestBody>(proxyRequestBody),
            responseType
        ).body

        return styxResponse?.body!!
    }

    private fun checkOnError(responseEntity: ResponseEntity<*>) {
        val series = responseEntity.statusCode.series()
        if (series == Series.CLIENT_ERROR || series == Series.SERVER_ERROR) {
            throw YooKassaClientException("Unknown error during request")
        }
    }

    companion object {
        const val PAYMENTS_URL = "https://api.yookassa.ru/v3/payments"
        const val PAYMENT_STATUS_URL = "https://api.yookassa.ru/v3/payments/"
    }
}
