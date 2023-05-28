package dev.crashteam.keanalytics.repository.mongo

import dev.crashteam.keanalytics.domain.mongo.ProductChangeTimeSeries
import org.springframework.data.mongodb.repository.ReactiveMongoRepository
import org.springframework.stereotype.Repository

@Repository
interface ProductChangeTimeSeriesRepository : ReactiveMongoRepository<ProductChangeTimeSeries, String>,
    ProductChangeTimeSeriesCustomRepository
