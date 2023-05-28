package dev.crashteam.keanalytics.repository.mongo.pageable

class PageResult<T>(
    var result: List<T> = emptyList(),
    var pageSize: Long = 0,
    var page: Int = 0,
    var totalPages: Int = 0,
)
