package dev.crashteam.keanalytics.config.properties

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.ConstructorBinding
import org.springframework.validation.annotation.Validated
import javax.validation.constraints.NotEmpty
import javax.validation.constraints.NotNull

@Validated
@ConstructorBinding
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
