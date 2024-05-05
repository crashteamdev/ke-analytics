package dev.crashteam.keanalytics.config.properties

import jakarta.validation.constraints.NotEmpty
import jakarta.validation.constraints.NotNull
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.validation.annotation.Validated

@Validated
@ConfigurationProperties(prefix = "service")
data class ServiceProperties(
    @field:NotNull
    val proxy: ProxyServiceProperties? = null,
)

data class ProxyServiceProperties(
    @field:NotEmpty
    val url: String? = null,
)
