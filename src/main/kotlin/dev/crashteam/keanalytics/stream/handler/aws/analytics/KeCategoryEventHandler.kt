package dev.crashteam.keanalytics.stream.handler.aws.analytics

import dev.crashteam.ke.scrapper.data.v1.KeCategory
import dev.crashteam.ke.scrapper.data.v1.KeScrapperEvent
import dev.crashteam.keanalytics.db.model.tables.pojos.CategoryHierarchical
import dev.crashteam.keanalytics.repository.postgres.CategoryRepository
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import org.springframework.stereotype.Component

private val log = KotlinLogging.logger {}

@Component
class KeCategoryEventHandler(
    private val categoryRepository: CategoryRepository,
) : KeScrapEventHandler {

    override fun handle(events: List<KeScrapperEvent>) {
        runBlocking {
            val keCategoryChanges = events.map { it.eventPayload.keCategoryChange }
            try {
                for (keCategoryChange in keCategoryChanges) {
                    log.info {
                        "Consume category from stream." +
                                " categoryId=${keCategoryChange.category.categoryId};" +
                                " childCount=${keCategoryChange.category.childrenList?.size}"
                    }
                    saveHierarchicalRootCategory(keCategoryChange.category)
                }
            } catch (e: Exception) {
                log.error(e) { "Exception during handle category message" }
            }
        }
    }

    override fun isHandle(event: KeScrapperEvent): Boolean {
        return event.eventPayload.hasKeCategoryChange()
    }

    private suspend fun saveHierarchicalRootCategory(
        rootCategoryRecord: KeCategory,
    ) {
        val rootCategory = CategoryHierarchical(
            rootCategoryRecord.categoryId,
            0,
            rootCategoryRecord.title
        )
        log.info { "Save root category: $rootCategory" }
        categoryRepository.save(rootCategory)
        if (rootCategoryRecord.childrenList?.isNotEmpty() == true) {
            saveHierarchicalChildCategory(rootCategoryRecord, rootCategoryRecord.childrenList)
        }
    }

    private suspend fun saveHierarchicalChildCategory(
        currentCategoryRecord: KeCategory,
        childCategoryRecords: List<KeCategory>
    ) {
        for (childrenRecord in childCategoryRecords) {
            val childCategory = CategoryHierarchical(
                childrenRecord.categoryId,
                currentCategoryRecord.categoryId,
                childrenRecord.title
            )
            categoryRepository.save(childCategory)
            if (childrenRecord.childrenList?.isNotEmpty() == true) {
                saveHierarchicalChildCategory(childrenRecord, childrenRecord.childrenList)
            }
        }
    }
}
