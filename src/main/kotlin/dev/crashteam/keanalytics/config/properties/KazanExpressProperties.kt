package dev.crashteam.keanalytics.config.properties

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.ConstructorBinding
import org.springframework.validation.annotation.Validated
import javax.validation.constraints.NotEmpty

@Validated
@ConstructorBinding
@ConfigurationProperties(prefix = "kazanex")
data class KazanExpressProperties(
    @field:NotEmpty
    val groupCron: String? = null,
    @field:NotEmpty
    val productCron: String? = null,
    @field:NotEmpty
    val productCronV2: String? = null,
    @field:NotEmpty
    val aggregateCron: String? = null,
    @field:NotEmpty
    val sellerCron: String? = null,
    @field:NotEmpty
    val reportCleanUpCron: String? = null,
    @field:NotEmpty
    val reportGenerateCron: String? = null,
    @field:NotEmpty
    val productPositionCron: String? = null,
    @field:NotEmpty
    val pendingMessageCron: String? = null,
    val throttlingMs: Long? = null,
    val reportLiveTimeHours: String? = null,
)
