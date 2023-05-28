package dev.crashteam.keanalytics.config.properties

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.ConstructorBinding
import org.springframework.validation.annotation.Validated
import javax.validation.constraints.NotEmpty
import javax.validation.constraints.NotNull

@Validated
@ConstructorBinding
@ConfigurationProperties(prefix = "service")
data class ServiceProperties(
    @field:NotNull
    val proxy: ProxyServiceProperties? = null,
)

data class ProxyServiceProperties(
    @field:NotEmpty
    val url: String? = null,
)
