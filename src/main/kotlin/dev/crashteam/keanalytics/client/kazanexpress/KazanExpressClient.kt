package dev.crashteam.keanalytics.client.kazanexpress

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import dev.crashteam.keanalytics.client.kazanexpress.model.*
import dev.crashteam.keanalytics.client.kazanexpress.model.proxy.ProxyRequestBody
import dev.crashteam.keanalytics.client.kazanexpress.model.proxy.ProxyRequestContext
import dev.crashteam.keanalytics.client.kazanexpress.model.proxy.StyxResponse
import dev.crashteam.keanalytics.config.RedisConfig
import dev.crashteam.keanalytics.config.properties.ServiceProperties
import mu.KotlinLogging
import org.springframework.cache.annotation.Cacheable
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.HttpEntity
import org.springframework.http.HttpMethod
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.stereotype.Service
import org.springframework.web.client.RestTemplate
import java.util.*


private val log = KotlinLogging.logger {}

@Service
class KazanExpressClient(
    private val restTemplate: RestTemplate,
    private val serviceProperties: ServiceProperties,
) {

    fun getShopByName(shopName: String, size: Int = 500, page: Int = 0): ShopListResponse? {
        val proxyRequestBody = ProxyRequestBody(
            url = "$ROOT_URL/shop/$shopName/more?size=$size&page=$page",
            httpMethod = "GET",
            context = listOf(
                ProxyRequestContext(
                    key = "headers",
                    value = mapOf(
                        "User-Agent" to USER_AGENT,
                    )
                )
            )
        )
        val responseType: ParameterizedTypeReference<StyxResponse<ShopListResponse>> =
            object : ParameterizedTypeReference<StyxResponse<ShopListResponse>>() {}
        val styxResponse = restTemplate.exchange(
            "${serviceProperties.proxy!!.url}/v2/proxy",
            HttpMethod.POST,
            HttpEntity<ProxyRequestBody>(proxyRequestBody),
            responseType
        ).body

        return handleProxyResponse(styxResponse!!)
    }

    fun getCategoryInfo(categoryId: String): CategoryInfoResponse? {
        val proxyRequestBody = ProxyRequestBody(
            url = "$ROOT_URL/category/v2/$categoryId",
            httpMethod = "GET",
            context = listOf(
                ProxyRequestContext(
                    key = "headers",
                    value = mapOf(
                        "User-Agent" to USER_AGENT,
                        "x-iid" to UUID.randomUUID().toString()
                    )
                )
            )
        )
        val responseType: ParameterizedTypeReference<StyxResponse<CategoryInfoResponse>> =
            object : ParameterizedTypeReference<StyxResponse<CategoryInfoResponse>>() {}
        val styxResponse = restTemplate.exchange(
            "${serviceProperties.proxy!!.url}/v2/proxy",
            HttpMethod.POST,
            HttpEntity<ProxyRequestBody>(proxyRequestBody),
            responseType
        ).body

        return handleProxyResponse(styxResponse!!)
    }

    fun getCategoryGQL(
        categoryId: String,
        offset: Long,
        limit: Int
    ): KazanExpressGQLQueryResponse<CategoryGQLSearchResponse>? {
        val categoryGQLQuery = KazanExpressGQLQuery(
            operationName = "getMakeSearch",
            query = "query getMakeSearch(\$queryInput: MakeSearchQueryInput!) { makeSearch(query: \$queryInput) { id queryId queryText category { ...CategoryShortFragment __typename } categoryTree { category { ...CategoryFragment __typename } total __typename } items { catalogCard { __typename ...SkuGroupCardFragment } __typename } facets { ...FacetFragment __typename } total mayHaveAdultContent __typename } } fragment FacetFragment on Facet { filter { id title type measurementUnit description __typename } buckets { filterValue { id description image name __typename } total __typename } range { min max __typename } __typename } fragment CategoryFragment on Category { id icon parent { id __typename } seo { header metaTag __typename } title adult __typename } fragment CategoryShortFragment on Category { id parent { id title __typename } title __typename } fragment SkuGroupCardFragment on SkuGroupCard { ...DefaultCardFragment photos { key link(trans: PRODUCT_540) { high low __typename } previewLink: link(trans: PRODUCT_240) { high low __typename } __typename } badges { ... on BottomTextBadge { backgroundColor description id link text textColor __typename } __typename } characteristicValues { id value title characteristic { values { id title value __typename } title id __typename } __typename } __typename } fragment DefaultCardFragment on CatalogCard { adult favorite feedbackQuantity id minFullPrice minSellPrice offer { due icon text textColor __typename } badges { backgroundColor text textColor __typename } ordersQuantity productId rating title __typename }",
            variables = CategoryGQLQueryVariables(
                queryInput = CategoryGQLQueryInput(
                    categoryId = categoryId,
                    pagination = CategoryGQLQueryInputPagination(
                        offset = offset,
                        limit = limit
                    ),
                    showAdultContent = "TRUE",
                    sort = "BY_RELEVANCE_DESC"
                )
            )
        )
        val query = jacksonObjectMapper().writeValueAsBytes(categoryGQLQuery)
        val proxyRequestBody = ProxyRequestBody(
            url = "https://dshop.kznexpress.ru",
            httpMethod = "POST",
            context = listOf(
                ProxyRequestContext(
                    key = "headers",
                    value = mapOf(
                        "User-Agent" to USER_AGENT,
                        "Authorization" to "Basic $AUTH_TOKEN",
                        "Content-Type" to MediaType.APPLICATION_JSON_VALUE,
                        "x-iid" to UUID.randomUUID().toString()
                    )
                ),
                ProxyRequestContext("content", Base64.getEncoder().encodeToString(query))
            )
        )
        val responseType: ParameterizedTypeReference<StyxResponse<KazanExpressGQLQueryResponse<CategoryGQLSearchResponse>>> =
            object :
                ParameterizedTypeReference<StyxResponse<KazanExpressGQLQueryResponse<CategoryGQLSearchResponse>>>() {}
        val styxResponse = restTemplate.exchange(
            "${serviceProperties.proxy!!.url}/v2/proxy",
            HttpMethod.POST,
            HttpEntity<ProxyRequestBody>(proxyRequestBody),
            responseType
        ).body

        return handleProxyResponse(styxResponse!!)!!
    }

    fun getRootCategories(): RootCategoriesResponse? {
        val proxyRequestBody = ProxyRequestBody(
            url = "$ROOT_URL/main/root-categories?eco=false",
            httpMethod = "GET",
            context = listOf(
                ProxyRequestContext(
                    key = "headers",
                    value = mapOf(
                        "User-Agent" to USER_AGENT,
                        "Authorization" to "Basic $AUTH_TOKEN"
                    )
                )
            )
        )
        val responseType: ParameterizedTypeReference<StyxResponse<RootCategoriesResponse>> =
            object : ParameterizedTypeReference<StyxResponse<RootCategoriesResponse>>() {}
        val styxResponse = restTemplate.exchange(
            "${serviceProperties.proxy!!.url}/v2/proxy",
            HttpMethod.POST,
            HttpEntity<ProxyRequestBody>(proxyRequestBody),
            responseType
        ).body

        return handleProxyResponse(styxResponse!!)
    }

    @Cacheable(value = [RedisConfig.KE_CLIENT_CACHE_NAME], key = "#productId", unless = "#result == null")
    fun getProductInfo(productId: String): ProductResponse? {
        val proxyRequestBody = ProxyRequestBody(
            url = "$ROOT_URL/v2/product/$productId",
            httpMethod = "GET",
            context = listOf(
                ProxyRequestContext(
                    key = "headers",
                    value = mapOf(
                        "User-Agent" to USER_AGENT,
                        "Authorization" to "Basic $AUTH_TOKEN"
                    )
                )
            )
        )
        val responseType: ParameterizedTypeReference<StyxResponse<ProductResponse>> =
            object : ParameterizedTypeReference<StyxResponse<ProductResponse>>() {}
        val styxResponse = restTemplate.exchange(
            "${serviceProperties.proxy!!.url}/v2/proxy",
            HttpMethod.POST,
            HttpEntity<ProxyRequestBody>(proxyRequestBody),
            responseType
        ).body

        return handleProxyResponse(styxResponse!!)
    }

    fun getSellerProductsGQL(
        sellerId: Long,
        limit: Long = 48,
        offset: Long = 0
    ): ShopGQLQueryResponse? {
        val shopGQLQuery = KazanExpressGQLQuery(
            operationName = "getMakeSearch",
            query = "query getMakeSearch(\$queryInput: MakeSearchQueryInput!) {\n  makeSearch(query: \$queryInput) {\n    id\n    queryId\n    queryText\n    category {\n      ...CategoryShortFragment\n      __typename\n    }\n    categoryTree {\n      category {\n        ...CategoryFragment\n        __typename\n      }\n      total\n      __typename\n    }\n    items {\n      catalogCard {\n        __typename\n        ...SkuGroupCardFragment\n      }\n      __typename\n    }\n    facets {\n      ...FacetFragment\n      __typename\n    }\n    total\n    mayHaveAdultContent\n    __typename\n  }\n}\n\nfragment FacetFragment on Facet {\n  filter {\n    id\n    title\n    type\n    measurementUnit\n    description\n    __typename\n  }\n  buckets {\n    filterValue {\n      id\n      description\n      image\n      name\n      __typename\n    }\n    total\n    __typename\n  }\n  range {\n    min\n    max\n    __typename\n  }\n  __typename\n}\n\nfragment CategoryFragment on Category {\n  id\n  icon\n  parent {\n    id\n    __typename\n  }\n  seo {\n    header\n    metaTag\n    __typename\n  }\n  title\n  adult\n  __typename\n}\n\nfragment CategoryShortFragment on Category {\n  id\n  parent {\n    id\n    title\n    __typename\n  }\n  title\n  __typename\n}\n\nfragment SkuGroupCardFragment on SkuGroupCard {\n  ...DefaultCardFragment\n  photos {\n    key\n    link(trans: PRODUCT_540) {\n      high\n      low\n      __typename\n    }\n    previewLink: link(trans: PRODUCT_240) {\n      high\n      low\n      __typename\n    }\n    __typename\n  }\n  badges {\n    ... on BottomTextBadge {\n      backgroundColor\n      description\n      id\n      link\n      text\n      textColor\n      __typename\n    }\n    __typename\n  }\n  characteristicValues {\n    id\n    value\n    title\n    characteristic {\n      values {\n        id\n        title\n        value\n        __typename\n      }\n      title\n      id\n      __typename\n    }\n    __typename\n  }\n  __typename\n}\n\nfragment DefaultCardFragment on CatalogCard {\n  adult\n  favorite\n  feedbackQuantity\n  id\n  minFullPrice\n  minSellPrice\n  offer {\n    due\n    icon\n    text\n    textColor\n    __typename\n  }\n  badges {\n    backgroundColor\n    text\n    textColor\n    __typename\n  }\n  ordersQuantity\n  productId\n  rating\n  title\n  __typename\n}",
            variables = ShopGQLQueryVariables(
                queryInput = ShopGQLQueryInput(
                    shopId = sellerId.toString(),
                    pagination = ShopGQLPagination(
                        offset = offset,
                        limit = limit
                    ),
                    showAdultContent = "TRUE",
                    sort = "BY_RELEVANCE_DESC"
                )
            )
        )
        val query = jacksonObjectMapper().writeValueAsBytes(shopGQLQuery)
        val proxyRequestBody = ProxyRequestBody(
            url = "https://dshop.kznexpress.ru",
            httpMethod = "POST",
            context = listOf(
                ProxyRequestContext(
                    key = "headers",
                    value = mapOf(
                        "User-Agent" to USER_AGENT,
                        "Authorization" to "Basic $AUTH_TOKEN",
                        "Content-Type" to MediaType.APPLICATION_JSON_VALUE,
                        "x-iid" to UUID.randomUUID().toString()
                    )
                ),
                ProxyRequestContext("content", Base64.getEncoder().encodeToString(query))
            )
        )
        val responseType: ParameterizedTypeReference<StyxResponse<KazanExpressGQLQueryResponse<ShopGQLQueryResponse>>> =
            object : ParameterizedTypeReference<StyxResponse<KazanExpressGQLQueryResponse<ShopGQLQueryResponse>>>() {}
        val styxResponse = restTemplate.exchange(
            "${serviceProperties.proxy!!.url}/v2/proxy",
            HttpMethod.POST,
            HttpEntity<ProxyRequestBody>(proxyRequestBody),
            responseType
        ).body

        return handleProxyResponse(styxResponse!!)!!.data?.makeSearch
    }

    private fun <T> handleProxyResponse(styxResponse: StyxResponse<T>): T? {
        val originalStatus = styxResponse.originalStatus
        val statusCode = HttpStatus.resolve(originalStatus)
            ?: throw IllegalStateException("Unknown http status: $originalStatus")
        val isError = statusCode.series() == HttpStatus.Series.CLIENT_ERROR
                || statusCode.series() == HttpStatus.Series.SERVER_ERROR
        if (isError) {
            throw KazanExpressClientException(
                originalStatus,
                styxResponse.body.toString(),
                "Bad response from KazanExpress. Status=$originalStatus; Body=${styxResponse.body.toString()}"
            )
        }
        if (styxResponse.code != 0) {
            log.warn { "Bad proxy status - ${styxResponse.code}" }
        }
        return styxResponse.body
    }

    companion object {
        private const val ROOT_URL = "https://api.kazanexpress.ru/api"
        private const val AUTH_TOKEN = "a2F6YW5leHByZXNzLWN1c3RvbWVyOmN1c3RvbWVyU2VjcmV0S2V5"
        private const val USER_AGENT =
            "Mozilla/5.0 (Linux; Android 10; SM-A205U) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.115 Mobile Safari/537.36"
    }
}
