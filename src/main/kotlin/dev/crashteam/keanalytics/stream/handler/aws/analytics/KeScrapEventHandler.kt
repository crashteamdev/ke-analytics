package dev.crashteam.keanalytics.stream.handler.aws.analytics

import dev.crashteam.ke.scrapper.data.v1.KeScrapperEvent

interface KeScrapEventHandler {

    fun handle(events: List<KeScrapperEvent>)

    fun isHandle(event: KeScrapperEvent): Boolean
}
