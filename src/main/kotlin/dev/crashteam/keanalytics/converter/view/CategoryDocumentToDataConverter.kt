package dev.crashteam.keanalytics.converter.view

import dev.crashteam.keanalytics.controller.model.CategoryView
import dev.crashteam.keanalytics.converter.DataConverter
import dev.crashteam.keanalytics.domain.mongo.CategoryDocument
import org.springframework.stereotype.Component

@Component
class CategoryDocumentToDataConverter : DataConverter<CategoryDocument, CategoryView> {

    override fun convert(category: CategoryDocument): CategoryView {
        return convertToView(category)
    }

    private fun convertToView(category: CategoryDocument): CategoryView {
        return CategoryView().apply {
            categoryId = category.publicId
            title = category.title
            adult = category.adult
            eco = category.eco
            productAmount = category.productAmount
            child = category.document.map { convertToView(it) }
        }
    }
}
