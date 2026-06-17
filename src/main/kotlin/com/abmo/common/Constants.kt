package com.abmo.common

object Constants {

    /**
     * The default maximum number of concurrent downloads allowed.
     */
    const val DEFAULT_CONCURRENT_DOWNLOAD_LIMIT = 4

    /**
     * Toggle for verbose logging.
     * Set to `true` to enable detailed logs, `false` to disable.
     */
    var VERBOSE = false

    //
    var COEG = false

    // untuk delete folder segment
    var DELF = false

    const val ABYSS_BASE_URL = "https://abysscdn.com"
    val abyssDefaultHeaders = mapOf(
        "Referer" to "$ABYSS_BASE_URL/",
        "Origin" to ABYSS_BASE_URL
    )

}
