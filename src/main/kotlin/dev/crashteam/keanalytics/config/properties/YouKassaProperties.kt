package dev.crashteam.keanalytics.config.properties

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.ConstructorBinding
import org.springframework.validation.annotation.Validated
import javax.validation.constraints.NotEmpty

@Validated
@ConstructorBinding
@ConfigurationProperties(prefix = "youkassa")
data class YouKassaProperties(
    @field:NotEmpty
    val shopId: String? = null,
    @field:NotEmpty
    val key: String? = null,
)
