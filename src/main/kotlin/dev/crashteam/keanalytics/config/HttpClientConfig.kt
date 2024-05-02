package dev.crashteam.keanalytics.config

import org.apache.hc.client5.http.classic.HttpClient
import org.apache.hc.client5.http.config.ConnectionConfig
import org.apache.hc.client5.http.config.RequestConfig
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder
import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactoryBuilder
import org.apache.hc.core5.util.TimeValue
import org.apache.hc.core5.util.Timeout
import org.apache.http.client.config.CookieSpecs
import org.apache.http.conn.ssl.TrustStrategy
import org.apache.http.ssl.SSLContexts
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory
import java.security.cert.X509Certificate
import java.util.concurrent.TimeUnit
import javax.net.ssl.SSLContext

@Configuration
class HttpClientConfig {

    @Bean
    fun sslFactory(): org.apache.hc.client5.http.ssl.SSLConnectionSocketFactory? {
        val acceptingTrustStrategy =
            TrustStrategy { chain: Array<X509Certificate?>?, authType: String? -> true }

        val sslContext: SSLContext = SSLContexts.custom()
            .loadTrustMaterial(null, acceptingTrustStrategy)
            .build()

        return SSLConnectionSocketFactoryBuilder.create().setSslContext(sslContext).build()
    }

    @Bean
    fun simpleHttpClient(sslFactory: org.apache.hc.client5.http.ssl.SSLConnectionSocketFactory): HttpClient {
        val requestConfig = RequestConfig.custom()
            .setConnectionRequestTimeout(Timeout.ofMilliseconds(REQUEST_TIMEOUT))
            .setConnectTimeout(Timeout.ofMilliseconds(CONNECT_TIMEOUT))
            .setCookieSpec(CookieSpecs.STANDARD)
            .build()
        val socketConfig =
            org.apache.hc.core5.http.io.SocketConfig.custom().setSoTimeout(Timeout.ofMilliseconds(SOCKET_TIMEOUT))
                .build()
        val connectionManager = PoolingHttpClientConnectionManagerBuilder.create()
            .setDefaultSocketConfig(socketConfig)
            .setDefaultConnectionConfig(ConnectionConfig.custom().setTimeToLive(60, TimeUnit.SECONDS).build())
            .setSSLSocketFactory(sslFactory).build()
        return org.apache.hc.client5.http.impl.classic.HttpClients.custom()
            .setConnectionManager(connectionManager)
            .setDefaultRequestConfig(requestConfig)
            .disableAutomaticRetries()
            .evictExpiredConnections()
            .evictIdleConnections(TimeValue.ofSeconds(CLOSE_IDLE_CONNECTION_WAIT_TIME_SECS))
            .setDefaultRequestConfig(
                RequestConfig.custom().setCircularRedirectsAllowed(true).build()
            )
            .disableCookieManagement()
            .build()
    }

    @Bean
    fun simpleHttpRequestFactory(simpleHttpClient: HttpClient): HttpComponentsClientHttpRequestFactory {
        val requestFactory = HttpComponentsClientHttpRequestFactory()
        requestFactory.httpClient = simpleHttpClient
        return requestFactory
    }

    companion object {
        private const val CONNECT_TIMEOUT = 30000L
        private const val REQUEST_TIMEOUT = 30000L
        private const val SOCKET_TIMEOUT = 30000L
        private const val CLOSE_IDLE_CONNECTION_WAIT_TIME_SECS = 30L
    }
}
