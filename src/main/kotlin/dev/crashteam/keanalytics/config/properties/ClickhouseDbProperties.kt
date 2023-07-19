package dev.crashteam.keanalytics.config.properties

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.ConstructorBinding
import org.springframework.validation.annotation.Validated
import javax.validation.constraints.NotEmpty

@Validated
@ConstructorBinding
@ConfigurationProperties(prefix = "clickhouse")
data class ClickHouseDbProperties(
    @field:NotEmpty
    val url: String? = null,
    @field:NotEmpty
    val user: String? = null,
    val password: String? = null,
    val connectionTimeout: Long = 50000,
    val compress: Boolean = false,
    val ssl: Boolean = false,
)
