package dev.crashteam.keanalytics.stream.listener

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import dev.crashteam.keanalytics.client.kazanexpress.model.RootCategoriesResponse
import dev.crashteam.keanalytics.client.kazanexpress.model.SimpleCategory
import dev.crashteam.keanalytics.domain.mongo.CategoryDocument
import dev.crashteam.keanalytics.repository.mongo.CategoryDao
import dev.crashteam.keanalytics.stream.model.KeCategoryStreamRecord
import org.springframework.context.ApplicationContext
import org.springframework.data.redis.connection.stream.ObjectRecord
import org.springframework.data.redis.stream.StreamListener
import org.springframework.stereotype.Component
import java.time.LocalDateTime

private val log = KotlinLogging.logger {}

@Component
class KeCategoryStreamListener(
    private val objectMapper: ObjectMapper,
    private val categoryDao: CategoryDao
) : StreamListener<String, ObjectRecord<String, String>> {

    override fun onMessage(message: ObjectRecord<String, String>) {
        runBlocking {
            try {
                val categoryStreamRecord = objectMapper.readValue<KeCategoryStreamRecord>(message.value)
                log.info { "Consume category record from stream." +
                        " categoryId=${categoryStreamRecord.id} childCount=${categoryStreamRecord.children?.size}" }
                val categoryDocument = CategoryDocument(
                    categoryStreamRecord.id,
                    null,
                    categoryStreamRecord.adult,
                    categoryStreamRecord.eco,
                    categoryStreamRecord.title.trim(),
                    null,
                    LocalDateTime.now()
                )
                categoryDao.saveCategory(categoryDocument).awaitSingleOrNull()
                saveChildCategories(categoryStreamRecord, categoryStreamRecord.children ?: emptyList(), null)
            } catch (e: Exception) {
                log.error(e) { "Exception during handle category message" }
            }
        }
    }

    private suspend fun saveChildCategories(
        rootCategory: KeCategoryStreamRecord,
        children: List<KeCategoryStreamRecord>,
        path: String?,
    ) {
        for (childCategory in children) {
            val categoryDocument = CategoryDocument(
                childCategory.id,
                null,
                childCategory.adult,
                childCategory.eco,
                childCategory.title.trim(),
                if (path == null) {
                    ",${rootCategory.title.trim()},${childCategory.title.trim()},"
                } else "$path${childCategory.title.trim()},",
                LocalDateTime.now()
            )
            categoryDao.saveCategory(categoryDocument).awaitSingleOrNull()
            if (childCategory.children?.isNotEmpty() == true) {
                saveChildCategories(childCategory, childCategory.children, categoryDocument.path)
            }
        }
    }
}
