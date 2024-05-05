package dev.crashteam.keanalytics.config.properties

import jakarta.validation.constraints.NotEmpty
import jakarta.validation.constraints.NotNull
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.validation.annotation.Validated

@Validated
@ConfigurationProperties(prefix = "freekassa")
data class FreeKassaProperties(
    @field:NotEmpty
    val baseUrl: String? = null,
    @field:NotNull
    val shopId: Long? = null,
    @field:NotEmpty
    val apiKey: String? = null,
    @field:NotEmpty
    val secretWordFirst: String? = null,
    @field:NotEmpty
    val secretWordSecond: String? = null,
)
