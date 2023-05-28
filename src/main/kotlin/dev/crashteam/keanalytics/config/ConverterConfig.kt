package dev.crashteam.keanalytics.config

import dev.crashteam.keanalytics.controller.converter.DataConverter
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import org.springframework.context.support.ConversionServiceFactoryBean
import org.springframework.format.Formatter
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.*

@Configuration
@ComponentScan(
    basePackages = [
        "dev.crashteam.keanalytics.controller.converter",
        "dev.crashteam.keanalytics.domain.mongo.converter"
    ]
)
class ConverterConfig {

    @Bean
    @Primary
    fun conversionServiceFactoryBean(converters: Set<DataConverter<*, *>>): ConversionServiceFactoryBean {
        val conversionServiceFactoryBean = ConversionServiceFactoryBean()
        conversionServiceFactoryBean.setConverters(converters)

        return conversionServiceFactoryBean
    }

    @Bean
    fun localDateFormatter(): Formatter<LocalDate> {
        return object : Formatter<LocalDate> {
            override fun parse(text: String, locale: Locale): LocalDate {
                return LocalDate.parse(text, DateTimeFormatter.ISO_DATE)
            }

            override fun print(`object`: LocalDate, locale: Locale): String {
                return DateTimeFormatter.ISO_DATE.format(`object`)
            }
        }
    }
}
