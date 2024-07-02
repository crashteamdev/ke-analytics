package dev.crashteam.keanalytics.extensions

import dev.crashteam.keanalytics.db.model.enums.SubscriptionType
import dev.crashteam.keanalytics.domain.*

fun Int.mapToSubscription(): UserSubscription? {
    return UserSubscription::class.sealedSubclasses.firstOrNull {
        it.objectInstance?.num == this
    }?.objectInstance
}

fun String.mapToSubscription(): UserSubscription? {
    return UserSubscription::class.sealedSubclasses.firstOrNull {
        it.objectInstance?.name == this
    }?.objectInstance
}

fun SubscriptionType.mapToUserSubscription(): UserSubscription {
    return when (this) {
        SubscriptionType.default_ -> DefaultSubscription

        SubscriptionType.advanced -> AdvancedSubscription

        SubscriptionType.pro -> ProSubscription

        SubscriptionType.demo -> DemoSubscription
    }
}

fun String.mapToEntityUserSubscription(): SubscriptionType {
    return when (this) {
        "default" -> SubscriptionType.default_
        "advanced" -> SubscriptionType.advanced
        "pro" -> SubscriptionType.pro
        else -> throw IllegalArgumentException("Unknown type: $this")
    }
}

