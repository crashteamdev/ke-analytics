package dev.crashteam.keanalytics.extensions

import org.quartz.JobExecutionContext
import org.quartz.JobExecutionException
import org.springframework.context.ApplicationContext

fun JobExecutionContext.getApplicationContext(): ApplicationContext {
    return this.scheduler.context["applicationContext"] as? ApplicationContext
        ?: throw JobExecutionException("No application context available in scheduler context")
}
