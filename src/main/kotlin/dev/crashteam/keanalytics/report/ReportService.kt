package dev.crashteam.keanalytics.report

import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactor.awaitSingleOrNull
import dev.crashteam.keanalytics.domain.mongo.ReportStatus
import dev.crashteam.keanalytics.repository.mongo.ReportRepository
import org.springframework.data.redis.core.ReactiveRedisCallback
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.io.InputStream
import java.nio.ByteBuffer
import java.time.Duration


@Service
class ReportService(
    private val reportFileService: ReportFileService,
    private val reportRepository: ReportRepository,
    private val redisTemplate: ReactiveRedisTemplate<String, Long>,
) {

    @Transactional
    suspend fun saveSellerReportV2(sellerLink: String, interval: Int, jobId: String, reportInputStream: InputStream) {
        val reportId: String =
            reportFileService.saveSellerReport(sellerLink, jobId, reportInputStream, "$sellerLink-$interval.xlsx")
        reportRepository.updateReportStatus(jobId, ReportStatus.COMPLETED).awaitSingleOrNull()
        reportRepository.setReportId(jobId, reportId).awaitSingleOrNull()
    }

    @Transactional
    suspend fun saveSellerReport(sellerLink: String, interval: Int, jobId: String, report: ByteArray) {
        val reportId: String =
            reportFileService.saveSellerReport(sellerLink, jobId, report.inputStream(), "$sellerLink-$interval.xlsx")
        reportRepository.updateReportStatus(jobId, ReportStatus.COMPLETED).awaitSingleOrNull()
        reportRepository.setReportId(jobId, reportId).awaitSingleOrNull()
    }

    @Transactional
    suspend fun saveCategoryReportV2(
        categoryPublicId: Long,
        categoryPath: List<String>,
        interval: Int,
        jobId: String,
        reportInputStream: InputStream
    ) {
        val reportId: String = reportFileService.saveCategoryReport(
            categoryPublicId,
            categoryPath.joinToString(","),
            jobId,
            reportInputStream,
            "$categoryPublicId-$interval.xlsx"
        )
        reportRepository.updateReportStatus(jobId, ReportStatus.COMPLETED).awaitSingleOrNull()
        reportRepository.setReportId(jobId, reportId).awaitSingleOrNull()
    }

    @Transactional
    suspend fun saveCategoryReport(
        categoryPublicId: Long,
        categoryTitle: String,
        interval: Int,
        jobId: String,
        report: ByteArray
    ) {
        val reportId: String = reportFileService.saveCategoryReport(
            categoryPublicId,
            categoryTitle,
            jobId,
            report.inputStream(),
            "$categoryTitle-$interval.xlsx"
        )
        reportRepository.updateReportStatus(jobId, ReportStatus.COMPLETED).awaitSingleOrNull()
        reportRepository.setReportId(jobId, reportId).awaitSingleOrNull()
    }

    suspend fun incrementShopUserReportCount(userid: String) {
        val key = "${SHOP_REPORT_PREFIX}-$userid"
        incrementUserReportCount(key)
    }

    fun incrementShopUserReportCountV2(userid: String): Flux<List<Any>> {
        val key = "${SHOP_REPORT_PREFIX}-$userid"
        return incrementUserReportCountSimple(key)
    }

    suspend fun decrementShopUserReportCount(userid: String) {
        val key = "${SHOP_REPORT_PREFIX}-$userid"
        decrementUserReportCount(key)
    }

    suspend fun getUserShopReportDailyReportCount(userId: String): Long? {
        val key = "${SHOP_REPORT_PREFIX}-$userId"
        return redisTemplate.opsForValue().get(key).awaitSingleOrNull()
    }

    fun getUserShopReportDailyReportCountV2(userId: String): Mono<Long> {
        val key = "${SHOP_REPORT_PREFIX}-$userId"
        return redisTemplate.opsForValue().get(key)
    }


    suspend fun incrementCategoryUserReportCount(userid: String) {
        val key = "${CATEGORY_REPORT_PREFIX}-$userid"
        incrementUserReportCount(key)
    }

    fun incrementCategoryUserReportCountV2(userid: String): Flux<List<Any>> {
        val key = "${CATEGORY_REPORT_PREFIX}-$userid"
        return incrementUserReportCountSimple(key)
    }

    suspend fun decrementCategoryUserReportCount(userId: String) {
        val key = "${CATEGORY_REPORT_PREFIX}-$userId"
        decrementUserReportCount(key)
    }

    suspend fun getUserCategoryReportDailyReportCount(userId: String): Long? {
        val key = "${CATEGORY_REPORT_PREFIX}-$userId"
        return redisTemplate.opsForValue().get(key).awaitSingleOrNull()
    }

    fun getUserCategoryReportDailyReportCountV2(userId: String): Mono<Long> {
        val key = "${CATEGORY_REPORT_PREFIX}-$userId"
        return redisTemplate.opsForValue().get(key)
    }

    private suspend fun incrementUserReportCount(key: String): List<Any>? {
        return redisTemplate.execute(ReactiveRedisCallback<List<Any>> { connection ->
            val bbKey = ByteBuffer.wrap(key.toByteArray())
            Mono.zip(
                connection.numberCommands().incr(bbKey),
                connection.keyCommands().expire(bbKey, Duration.ofHours(24))
            ).then(Mono.empty())
        }).awaitFirstOrNull()
    }

    private fun incrementUserReportCountSimple(key: String): Flux<List<Any>> {
        return redisTemplate.execute { connection ->
            val bbKey = ByteBuffer.wrap(key.toByteArray())
            Mono.zip(
                connection.numberCommands().incr(bbKey),
                connection.keyCommands().expire(bbKey, Duration.ofHours(24))
            ).then(Mono.empty())
        }
    }

    private suspend fun decrementUserReportCount(key: String): List<Any>? {
        return redisTemplate.execute(ReactiveRedisCallback<List<Any>> { connection ->
            val bbKey = ByteBuffer.wrap(key.toByteArray())
            Mono.zip(
                connection.numberCommands().decr(bbKey),
                connection.keyCommands().expire(bbKey, Duration.ofHours(24))
            ).then(Mono.empty())
        }).awaitFirstOrNull()
    }

    private companion object {
        const val SHOP_REPORT_PREFIX = "shop-report"
        const val CATEGORY_REPORT_PREFIX = "category-report"
    }

}
