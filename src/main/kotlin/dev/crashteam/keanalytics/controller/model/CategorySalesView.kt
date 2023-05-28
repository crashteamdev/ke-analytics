package dev.crashteam.keanalytics.controller.model

import java.math.BigDecimal

class CategorySalesViewWrapper {
    var data: List<CategorySalesView>? = null
    var page: Int? = null
    var pageSize: Int? = null
    var totalPages: Int? = null
}

class CategorySalesView {
    var productId: Long? = null
    var skuId: Long? = null
    var name: String? = null
    var seller: CategorySalesSellerView? = null
    var category: CategoryDataView? = null
    var availableAmount: Long? = null
    var price: BigDecimal? = null
    var proceeds: BigDecimal? = null
    var priceGraph: List<Long>? = null
    var orderGraph: List<Long>? = null
    var daysInStock: Int? = null
}

class CategorySalesSellerView {
    var id: Long? = null
    var name: String? = null
}

class CategoryDataView {
    var name: String? = null
}
