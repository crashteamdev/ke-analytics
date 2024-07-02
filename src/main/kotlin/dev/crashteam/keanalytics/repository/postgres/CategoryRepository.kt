package dev.crashteam.keanalytics.repository.postgres

import dev.crashteam.keanalytics.db.model.tables.pojos.CategoryHierarchical

interface CategoryRepository {

    fun save(categoryHierarchical: CategoryHierarchical)

    fun findByPublicId(publicId: Long): CategoryHierarchical?

}
