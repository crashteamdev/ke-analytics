package dev.crashteam.keanalytics.controller.model

import com.fasterxml.jackson.annotation.JsonProperty

data class SimilarItemView(
    @JsonProperty("productId")
    val productId: Long,
    @JsonProperty("skuId")
    val skuId: Long,
    @JsonProperty("name")
    val name: String,
    @JsonProperty("photoKey")
    val photoKey: String
)
