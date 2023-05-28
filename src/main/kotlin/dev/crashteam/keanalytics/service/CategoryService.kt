package dev.crashteam.keanalytics.service

import dev.crashteam.keanalytics.domain.mongo.CategoryDocument
import dev.crashteam.keanalytics.repository.mongo.CategoryRepository
import dev.crashteam.keanalytics.repository.mongo.model.CategoryAveragePriceAggregate
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.LocalDate

@Service
class CategoryService(
    private val categoryRepository: CategoryRepository,
) {

    fun getAllCategories(): Flux<CategoryDocument> {
        return categoryRepository.findByPathIsNull().map { rootCategory ->
            categoryRepository.findByPathOrderByPath("^,${rootCategory.title},").collectList().map {
                buildCategory(rootCategory, it)
            }
        }.flatMapSequential { it }
    }

    fun getCategoryAveragePrice(
        categoryPath: List<String>,
        date: LocalDate
    ): Mono<CategoryAveragePriceAggregate> {
        return categoryRepository.aggregateCategoryAveragePrice(categoryPath, date)
    }

    private fun buildCategory(
        rootCategory: CategoryDocument,
        childCategories: MutableList<CategoryDocument>
    ): CategoryDocument {
        val map = HashMap<String, CategoryDocument>()
        for (childCategory in childCategories) {
            map[childCategory.path!!] = childCategory
        }
        for (childCategory in childCategories) {
            val path = childCategory.path!!.substring(0, childCategory.path.dropLast(1).lastIndexOf(",")) + ","
            if (path.replace(",", "") == rootCategory.title) {
                rootCategory.document.add(childCategory)
            } else {
                val parent = map[path]
                parent?.document?.add(childCategory)
            }
        }
        return rootCategory
    }
}
