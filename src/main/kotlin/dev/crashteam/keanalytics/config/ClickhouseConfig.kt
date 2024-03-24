package dev.crashteam.keanalytics.config

import com.clickhouse.client.config.ClickHouseClientOption
import com.clickhouse.client.config.ClickHouseDefaults
import com.clickhouse.jdbc.ClickHouseDataSource
import dev.crashteam.keanalytics.config.properties.ClickHouseDbProperties
import liquibase.integration.spring.SpringLiquibase
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.liquibase.LiquibaseProperties
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.jdbc.core.JdbcTemplate
import java.util.*
import javax.sql.DataSource

@Configuration
class ClickhouseConfig {

    @Bean
    fun clickHouseDataSource(clickHouseDbProperties: ClickHouseDbProperties): ClickHouseDataSource {
        val info = Properties()
        info.setProperty(ClickHouseDefaults.USER.key, clickHouseDbProperties.user)
        info.setProperty(ClickHouseDefaults.PASSWORD.key, clickHouseDbProperties.password)
        info.setProperty(ClickHouseClientOption.COMPRESS.key, clickHouseDbProperties.compress.toString())
        info.setProperty(
            ClickHouseClientOption.CONNECTION_TIMEOUT.key,
            clickHouseDbProperties.connectionTimeout.toString()
        )
        info.setProperty(
            ClickHouseClientOption.SOCKET_TIMEOUT.key,
            clickHouseDbProperties.socketTimeout.toString()
        )

        info.setProperty("ssl", clickHouseDbProperties.ssl.toString())
        return ClickHouseDataSource(clickHouseDbProperties.url, info)
    }

    @Bean
    @Autowired
    fun clickHouseJdbcTemplate(clickHouseDataSource: DataSource): JdbcTemplate {
        return JdbcTemplate(clickHouseDataSource)
    }

    @Bean
    @ConfigurationProperties(prefix = "clickhouse.liquibase")
    fun clickhouseLiquibaseProperties(): LiquibaseProperties {
        return LiquibaseProperties()
    }

    @Bean
    fun clickhouseLiquibase(
        clickHouseDataSource: ClickHouseDataSource,
        clickhouseLiquibaseProperties: LiquibaseProperties
    ): SpringLiquibase {
        return springLiquibase(clickHouseDataSource, clickhouseLiquibaseProperties)
    }

    private fun springLiquibase(dataSource: DataSource, properties: LiquibaseProperties): SpringLiquibase {
        val liquibase = SpringLiquibase()
        liquibase.dataSource = dataSource
        liquibase.changeLog = properties.changeLog
        liquibase.contexts = properties.contexts
        liquibase.defaultSchema = properties.defaultSchema
        liquibase.isDropFirst = properties.isDropFirst
        liquibase.setShouldRun(properties.isEnabled)
        liquibase.labels = properties.labels
        liquibase.setChangeLogParameters(properties.parameters)
        liquibase.setRollbackFile(properties.rollbackFile)
        return liquibase
    }


}
