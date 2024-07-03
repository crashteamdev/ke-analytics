package dev.crashteam.keanalytics.stream.listener.redis

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import dev.crashteam.keanalytics.repository.postgres.CategoryRepository
import dev.crashteam.keanalytics.db.model.tables.pojos.CategoryHierarchical
import dev.crashteam.keanalytics.stream.model.KeCategoryStreamRecord
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import org.springframework.data.redis.connection.stream.ObjectRecord
import org.springframework.data.redis.stream.StreamListener
import org.springframework.stereotype.Component

private val log = KotlinLogging.logger {}

@Component
class KeCategoryStreamListener(
    private val objectMapper: ObjectMapper,
    private val categoryRepository: CategoryRepository,
) : StreamListener<String, ObjectRecord<String, String>> {

    override fun onMessage(message: ObjectRecord<String, String>) {
        runBlocking {
            try {
                val categoryStreamRecord = objectMapper.readValue<KeCategoryStreamRecord>(message.value)
                log.info {
                    "Consume category record from stream." +
                            " categoryId=${categoryStreamRecord.id} childCount=${categoryStreamRecord.children?.size}"
                }
                saveHierarchicalRootCategory(categoryStreamRecord)
            } catch (e: Exception) {
                log.error(e) { "Exception during handle category message" }
            }
        }
    }

    private suspend fun saveHierarchicalRootCategory(
        rootCategoryRecord: KeCategoryStreamRecord,
    ) {
        val rootCategory = CategoryHierarchical(
            rootCategoryRecord.id,
            0,
            rootCategoryRecord.title
        )
        log.info { "Save root category: $rootCategory" }
        categoryRepository.save(rootCategory)
        if (rootCategoryRecord.children?.isNotEmpty() == true) {
            saveHierarchicalChildCategory(rootCategoryRecord, rootCategoryRecord.children)
        }
    }

    private suspend fun saveHierarchicalChildCategory(
        currentCategoryRecord: KeCategoryStreamRecord,
        childCategoryRecords: List<KeCategoryStreamRecord>
    ) {
        for (childrenRecord in childCategoryRecords) {
            val childCategory = CategoryHierarchical(
                childrenRecord.id,
                currentCategoryRecord.id,
                childrenRecord.title
            )
            categoryRepository.save(childCategory)
            if (childrenRecord.children?.isNotEmpty() == true) {
                saveHierarchicalChildCategory(childrenRecord, childrenRecord.children)
            }
        }
    }
}
