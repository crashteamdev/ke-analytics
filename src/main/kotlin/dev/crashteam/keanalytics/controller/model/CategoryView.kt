package dev.crashteam.keanalytics.controller.model

class CategoryView {
    var categoryId: Long? = null
    var title: String? = null
    var adult: Boolean? = null
    var eco: Boolean? = null
    var productAmount: Long? = null
    var child: List<CategoryView>? = null
}
