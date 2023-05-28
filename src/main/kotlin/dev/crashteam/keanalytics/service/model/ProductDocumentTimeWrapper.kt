package dev.crashteam.keanalytics.service.model

import dev.crashteam.keanalytics.domain.mongo.ProductDocument

class ProductDocumentTimeWrapper(
    val productDocument: ProductDocument,
    val time: Long,
)
