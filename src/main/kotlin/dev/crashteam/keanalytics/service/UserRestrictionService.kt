package dev.crashteam.keanalytics.service

import dev.crashteam.keanalytics.domain.mongo.ProSubscription
import dev.crashteam.keanalytics.domain.mongo.UserDocument
import dev.crashteam.keanalytics.extensions.mapToUserSubscription
import org.springframework.stereotype.Service
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.Month

@Service
class UserRestrictionService {

    fun checkShopReportAccess(user: UserDocument, currentReportCount: Int): RestrictionResult {
        if (user.subscription == null || user.subscription.endAt.isBefore(LocalDateTime.now())) {
            return RestrictionResult.PROHIBIT
        }
        val userSubscription = user.subscription.mapToUserSubscription()!!
        if (currentReportCount >= userSubscription.shopReports()) {
            return RestrictionResult.PROHIBIT
        }
        return RestrictionResult.PERMIT
    }

    fun checkCategoryReportAccess(user: UserDocument, currentReportCount: Int): RestrictionResult {
        if (user.subscription == null || user.subscription.endAt.isBefore(LocalDateTime.now())) {
            return RestrictionResult.PROHIBIT
        }
        val userSubscription = user.subscription.mapToUserSubscription()!!
        if (currentReportCount >= userSubscription.categoryReports()) {
            return RestrictionResult.PROHIBIT
        }
        return RestrictionResult.PERMIT
    }

    fun checkDaysAccess(user: UserDocument, daysRequestCount: Int): RestrictionResult {
        if (user.subscription == null || user.subscription.endAt.isBefore(LocalDateTime.now())) {
            return if (daysRequestCount <= DEFAULT_FREE_DAYS) {
                RestrictionResult.PERMIT
            } else RestrictionResult.PROHIBIT
        }
        val userSubscription = user.subscription.mapToUserSubscription()!!
        val days = userSubscription.days()
        if (!days.contains(daysRequestCount)) {
            return RestrictionResult.PROHIBIT
        }
        return RestrictionResult.PERMIT
    }

    fun checkDaysHistoryAccess(user: UserDocument, fromTime: LocalDateTime): RestrictionResult {
        if (user.subscription == null || user.subscription.endAt.isBefore(LocalDateTime.now())) {
            return if (fromTime.toLocalDate() < LocalDate.now().minusDays(DEFAULT_FREE_DAYS.toLong())) {
                RestrictionResult.PROHIBIT
            } else RestrictionResult.PERMIT
        }
        val userSubscription = user.subscription.mapToUserSubscription()!!
        val days = userSubscription.days()
        if (userSubscription != ProSubscription) {
            val minimalSubscriptionDay = LocalDate.now().minusDays(days.upperBound.value.get().toLong() + 1)
            if (fromTime.toLocalDate() < minimalSubscriptionDay) {
                return RestrictionResult.PROHIBIT
            }
        }
        return RestrictionResult.PERMIT
    }

    enum class RestrictionResult {
        PERMIT, PROHIBIT
    }

    companion object {
        const val DEFAULT_FREE_DAYS = 3
    }
}
