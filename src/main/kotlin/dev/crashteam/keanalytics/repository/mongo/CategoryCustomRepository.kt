package dev.crashteam.keanalytics.repository.mongo

import dev.crashteam.keanalytics.repository.mongo.model.CategoryAveragePriceAggregate
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.LocalDate

interface CategoryCustomRepository {
    fun aggregateCategoryAveragePrice(
        categoryPath: List<String>,
        date: LocalDate
    ): Mono<CategoryAveragePriceAggregate>
}
