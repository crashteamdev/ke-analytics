package dev.crashteam.keanalytics.domain.mongo

import org.springframework.data.mongodb.core.index.Indexed
import org.springframework.data.mongodb.core.mapping.Document
import java.time.LocalDateTime

@Document("referral_codes")
data class ReferralCodeDocument(
    @Indexed(unique = true)
    val userId: String,
    val code: String,
    val invited: List<ReferralInvitedUserDocument>?
)

data class ReferralInvitedUserDocument(
    val userId: String,
    val date: LocalDateTime,
    val subscription: String
)
