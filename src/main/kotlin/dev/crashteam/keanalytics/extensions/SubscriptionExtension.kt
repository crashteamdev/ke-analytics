package dev.crashteam.keanalytics.extensions

import dev.crashteam.keanalytics.domain.mongo.DefaultSubscription
import dev.crashteam.keanalytics.domain.mongo.SubscriptionDocument
import dev.crashteam.keanalytics.domain.mongo.UserSubscription

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

fun SubscriptionDocument.mapToUserSubscription(): UserSubscription? {
    if (this.type != null && this.subType == null) {
        return DefaultSubscription
    }
    return UserSubscription::class.sealedSubclasses.firstOrNull {
        it.objectInstance?.name == this.subType
    }?.objectInstance
}

