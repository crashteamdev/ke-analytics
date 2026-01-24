package dev.crashteam.keanalytics.repository.clickhouse

import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.clickhouse.ClickHouseContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

@Testcontainers
@SpringBootTest
abstract class AbstractClickHouseIntegrationTest {
    companion object {
        @Container
        val clickHouseContainer =
            ClickHouseContainer("clickhouse/clickhouse-server:latest")
                .withExposedPorts(8123, 9000)

        @JvmStatic
        @DynamicPropertySource
        fun properties(registry: DynamicPropertyRegistry) {
            registry.add("clickhouse.url") { clickHouseContainer.jdbcUrl }
            registry.add("clickhouse.user") { clickHouseContainer.username }
            registry.add("clickhouse.password") { clickHouseContainer.password }
            registry.add("clickhouse.liquibase.change-log") { "classpath:db/changelog/db.ch.changelog-main.yml" }
        }
    }
}
