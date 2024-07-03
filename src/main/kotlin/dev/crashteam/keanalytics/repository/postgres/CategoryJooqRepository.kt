package dev.crashteam.keanalytics.repository.postgres

import dev.crashteam.keanalytics.db.model.tables.CategoryHierarchical.CATEGORY_HIERARCHICAL
import dev.crashteam.keanalytics.db.model.tables.pojos.CategoryHierarchical
import org.jooq.DSLContext
import org.springframework.stereotype.Repository

@Repository
class CategoryJooqRepository(
    private val dsl: DSLContext
) : CategoryRepository {

    override fun save(categoryHierarchical: CategoryHierarchical) {
        val ch = CATEGORY_HIERARCHICAL
        dsl.insertInto(ch, ch.CATEGORY_ID, ch.PARENT_CATEGORY_ID, ch.TITLE)
            .values(categoryHierarchical.categoryId, categoryHierarchical.parentCategoryId, categoryHierarchical.title)
            .onConflict(ch.CATEGORY_ID)
            .doUpdate().set(
                mapOf(
                    ch.PARENT_CATEGORY_ID to categoryHierarchical.parentCategoryId,
                    ch.TITLE to categoryHierarchical.title,
                )
            ).execute()
    }

    override fun findByPublicId(publicId: Long): CategoryHierarchical? {
        val ch = CATEGORY_HIERARCHICAL
        return dsl.selectFrom(ch)
            .where(ch.CATEGORY_ID.eq(publicId))
            .fetchOneInto(CategoryHierarchical::class.java)
    }
}
