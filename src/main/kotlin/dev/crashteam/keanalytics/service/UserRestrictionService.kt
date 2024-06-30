package dev.crashteam.keanalytics.service

import dev.crashteam.keanalytics.db.model.tables.pojos.Users
import dev.crashteam.keanalytics.domain.ProSubscription
import dev.crashteam.keanalytics.extensions.mapToUserSubscription
import org.springframework.stereotype.Service
import java.time.LocalDate
import java.time.LocalDateTime

@Service
class UserRestrictionService {

    fun checkShopReportAccess(user: Users, currentReportCount: Int): RestrictionResult {
        if (user.subscriptionType == null || user.subscriptionEndAt.isBefore(LocalDateTime.now())) {
            return RestrictionResult.PROHIBIT
        }
        val userSubscription = user.subscriptionType.mapToUserSubscription()
        if (currentReportCount >= userSubscription.shopReports()) {
            return RestrictionResult.PROHIBIT
        }
        return RestrictionResult.PERMIT
    }

    fun checkCategoryReportAccess(user: Users, currentReportCount: Int): RestrictionResult {
        if (user.subscriptionType == null || user.subscriptionEndAt.isBefore(LocalDateTime.now())) {
            return RestrictionResult.PROHIBIT
        }
        val userSubscription = user.subscriptionType.mapToUserSubscription()
        if (currentReportCount >= userSubscription.categoryReports()) {
            return RestrictionResult.PROHIBIT
        }
        return RestrictionResult.PERMIT
    }

    fun checkDaysAccess(user: Users, daysRequestCount: Int): RestrictionResult {
        if (user.subscriptionType == null || user.subscriptionEndAt.isBefore(LocalDateTime.now())) {
            return RestrictionResult.PROHIBIT
        }
        val userSubscription = user.subscriptionType.mapToUserSubscription()
        val days = userSubscription.days()
        if (!days.contains(daysRequestCount)) {
            return RestrictionResult.PROHIBIT
        }
        return RestrictionResult.PERMIT
    }

    fun checkDaysHistoryAccess(user: Users, fromTime: LocalDateTime): RestrictionResult {
        if (user.subscriptionType == null || user.subscriptionEndAt.isBefore(LocalDateTime.now())) {
            return RestrictionResult.PROHIBIT
        }
        val userSubscription = user.subscriptionType.mapToUserSubscription()
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
}
