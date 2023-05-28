package dev.crashteam.keanalytics.controller.converter

import dev.crashteam.keanalytics.controller.model.ItemCharacteristicView
import dev.crashteam.keanalytics.domain.mongo.ProductSplitCharacteristicDocument
import org.springframework.stereotype.Component

@Component
class ProductSplitCharacteristicDocumentToDataConverter :
    DataConverter<ProductSplitCharacteristicDocument, ItemCharacteristicView> {

    override fun convert(source: ProductSplitCharacteristicDocument): ItemCharacteristicView {
        return ItemCharacteristicView(
            type = source.type,
            title = source.title,
            value = source.value
        )
    }
}
