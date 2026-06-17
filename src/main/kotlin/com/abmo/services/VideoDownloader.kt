package com.abmo.services

import com.abmo.common.Constants
import com.abmo.common.Logger
import com.abmo.crypto.CryptoHelper
import com.abmo.model.Config
import com.abmo.model.SimpleVideo
import com.abmo.model.Datas
import com.abmo.model.video.Mp4
import com.abmo.model.video.Video
import com.abmo.model.video.toSimpleVideo
import com.abmo.util.displayProgressBar
import com.abmo.util.toObject
import com.mashape.unirest.http.Unirest
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import org.jsoup.Jsoup
import org.koin.core.component.KoinComponent
import org.koin.core.component.inject
import java.io.File
import java.util.*
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

class VideoDownloader: KoinComponent {

    private val cryptoHelper: CryptoHelper by inject()
    private val httpClientManager: HttpClientManager by inject()

    companion object {
        private const val FRAGMENT_SIZE_IN_BYTES = 2097152L
    }

    /**
     * Downloads video segments in parallel and merges them into a single MP4 file.
     * This function uses coroutines for concurrent downloading with a limit on the number of concurrent downloads.
     *
     * @param config The configuration containing settings like output file path and connection limits, resolution.
     * @param videoMetadata The metadata of the video, used to generate segment data and the decryption key.
     * @throws Exception If there are errors during the download or file operations.
     */
    suspend fun downloadSegmentsInParallel(config: Config, videoMetadata: Mp4?) {
        val simpleVideo = videoMetadata?.toSimpleVideo(config.resolution)
        val segmentTokens = generateSegmentTokens(simpleVideo)

        val tempDir = initializeDownloadTempDir(config, simpleVideo, segmentTokens.size)

        val segmentsToDownload = segmentTokens.filter { (index, _) -> index in tempDir.second }.ifEmpty {
            segmentTokens
        }

        // reference: https://kotlinlang.org/api/kotlinx.coroutines/kotlinx-coroutines-core/kotlinx.coroutines.sync/-semaphore/
        // used to limit the number of concurrent coroutines executing the download tasks.
        val semaphore = Semaphore(config.connections)
        val totalSegments = segmentsToDownload.size
        val mediaSize = segmentsToDownload.size * FRAGMENT_SIZE_IN_BYTES
        val downloadedSegments = AtomicInteger(0)
        val totalBytesDownloaded = AtomicLong(0L)

        val startTime = System.currentTimeMillis()

        coroutineScope {
            val segmentUrlWrite = File("D:/Delphi/A-TEST-BATVIDEO/Output/A/segmentUrl.txt")

            segmentUrlWrite.printWriter().use { writer ->            
            
                val downloadJobs = segmentsToDownload.entries.mapIndexed { _, segmentToken ->
                    Logger.debug("simpleVideo?.url : ${simpleVideo?.url}")
                    Logger.debug("simpleVideo?.size : ${simpleVideo?.size}")
                    Logger.debug("segmentToken.value : ${segmentToken.value}")
                    val segmentUrl = "${simpleVideo?.url}/sora/${simpleVideo?.size}/${segmentToken.value}"
                    Logger.debug("segmentUrl : ${segmentUrl}")

                    // untuk dapetin segment.txt
                    if (Constants.GETSEG) {
                    writer.println("${segmentUrl}")
                    }                   
    
                    if (Constants.DLSEG) {
                        async(Dispatchers.IO) {
                            val index = segmentToken.key
                            semaphore.withPermit {
                                requestSegment(segmentUrl, segmentToken.value, index).collect { chunk ->
                                    File(tempDir.first, "segment_$index").appendBytes(chunk)
                                    totalBytesDownloaded.addAndGet(chunk.size.toLong())
                                }
                            }
                            downloadedSegments.incrementAndGet()
        
                        }
                    }
                    
                }
            
                // untuk nampilin progress bar?
                // blm tau cara fix nya gmn
                // jd sementara ndak usah pake progress bar
                /*if (Constants.DLSEG) {
                    val progressJob = launch {
                        var lastUpdateTime = System.currentTimeMillis()
                        while (isActive) {
                            lastUpdateTime = displayProgressBar(
                                mediaSize,
                                totalSegments,
                                totalBytesDownloaded.toLong(),
                                downloadedSegments.get(),
                                startTime,
                                lastUpdateTime
                            )
                            delay(1000)
                        }
                    }                
                    downloadJobs.awaitAll()
                    progressJob.cancel()   
                }*/
            } // writernya ditaro di akhir biar ndak eror ndak tau knp
        }
        
        println("\n")
        Logger.debug("All segments have been downloaded successfully!")
        Logger.info("merging segments into mp4 file...")

        if (Constants.DLSEG) {
        config.outputFile?.let { mergeSegmentsIntoMp4File(tempDir.first, it) }    
        }        

    }

    private fun mergeSegmentsIntoMp4File(segmentFolderPath: File, output: File) {
        val segmentFiles  = segmentFolderPath.listFiles { file -> file.name.startsWith("segment_") }
            ?.toList()
            ?.sortedBy { it.name.removePrefix("segment_").toIntOrNull() }
            ?: emptyList()
        segmentFiles.forEach {
            output.appendBytes(it.readBytes())
        }

        Logger.success("Segments merged successfully.")      

        if (segmentFolderPath.exists() && segmentFolderPath.isDirectory) {
            Logger.debug("folder: ${segmentFolderPath.absolutePath}")

            if (Constants.DELF) {
                val files = segmentFolderPath.listFiles()
    
                if (files != null) {
                    for (file in files) {
                        file.delete()
                    }
                }
    
                if (!segmentFolderPath.delete()) {
                    Logger.error("Failed to delete folder: ${segmentFolderPath.absolutePath}")
    //                Logger.info("Deleted temporary folder at: ${segmentFolderPath.absolutePath}")
                }
            }
        } else {
            Logger.error("Folder does not exist or is not a directory: ${segmentFolderPath.absolutePath}")
        }
    }


    /**
     * Sends an HTTP GET request to retrieve and decode video metadata.
     *
     * @param url The URL to send the GET request to.
     * @param headers A map of headers to include in the request.
     * @return The decoded video metadata, or null if the extraction or decoding fails.
     */
    fun getVideoMetaData(url: String, headers: Map<String, String?>?): Mp4? {
        val response = httpClientManager.makeHttpRequest(url, headers)
        val encryptedData = response?.body ?: return null
        val videoData = parseEncryptedMp4MetadataFromHtml(encryptedData)
        return videoData

    }


    private fun parseEncryptedMp4MetadataFromHtml(html: String): Mp4? {
       val jsCode = Jsoup.parse(html)
           .select("script")
           .find { it.html().contains("datas") }
           ?.html()


        if (jsCode == null) {
            Logger.error("No encoded media metadata found in the provided HTML.")
            return null
        }
        val datasRegex = Regex("""const\s+datas\s*=\s*"([^"]*)"""")
        val datas = datasRegex.find(jsCode)?.groups?.get(1)?.value
        Logger.debug("datas : ${datas}")
        val decodedDatas = String(Base64.getDecoder().decode(datas), Charsets.ISO_8859_1)
        Logger.debug("decodedDatas : ${decodedDatas}")
        val mediaMetadata = decodedDatas.toObject<Datas>()
        val encryptedMediaMetadata = mediaMetadata.media
        val encryptedMediaMetadataSize = encryptedMediaMetadata?.length
        Logger.debug("encryptedMediaMetadata : ${encryptedMediaMetadata}")
        Logger.debug("encryptedMediaMetadataSize : ${encryptedMediaMetadataSize}")

        if (encryptedMediaMetadata == null) {
            Logger.error("failed to get encrypted media")
            return null
        }

        val mediaKey = "${mediaMetadata.user_id}:${mediaMetadata.slug}:${mediaMetadata.md5_id}"
        Logger.debug("mediaKey : ${mediaKey}")
        val decryptionKeyS = cryptoHelper.getKey(mediaKey)
        Logger.debug("decryptionKeyS : ${decryptionKeyS}")
        val decryptionKey = cryptoHelper.getKey(mediaKey).toByteArray()
        Logger.debug("decryptionKey : ${decryptionKey}")

        // klo ada tambahan .toObject .mp4 seterus maka
        // ndak tau knp ndak bs diambil lagi nilai val nya
        // jd terpaksa harus bikin val baru lainnya
        // baru bs diambil nilainya
        val mediaSources0 = cryptoHelper.decryptString(encryptedMediaMetadata, decryptionKey)
        val mediaSourcesSize0 = mediaSources0?.length                               
        Logger.debug("mediaSourcesSize0 : ${mediaSourcesSize0}")
        
        val mediaSources = cryptoHelper.decryptString(encryptedMediaMetadata, decryptionKey)
            .toObject<Video>()
            .mp4?.copy(
                slug = mediaMetadata.slug,
                md5_id = mediaMetadata.md5_id
            )
        
        Logger.debug("mediaSources : ${mediaSources}")
        
        return mediaSources
    }

    private fun initializeDownloadTempDir(
        config: Config,
        simpleVideo: SimpleVideo?,
        totalSegments: Int
    ): Pair<File, List<Int>> {
        val tempFolderName = "temp_${simpleVideo?.slug}_${simpleVideo?.label}"
        // no need to check if path exists before creating temp folder we already did that in Main.kt
        val tempFolder = File(config.outputFile?.parentFile, tempFolderName)

        if (tempFolder.exists() && tempFolder.isDirectory) {
            Logger.info("Resuming download from temporary folder: $tempFolderName. Continuing from previously downloaded segments.")
            println("\n")
            val existingSegments = tempFolder.listFiles { file ->
                if (
                    file.isFile &&
                    file.name.matches(Regex("segment_\\d+")) &&
                    file.length() < FRAGMENT_SIZE_IN_BYTES) {
                    file.delete()
                }
                file.isFile && file.name.matches(Regex("segment_\\d+"))
            }?.mapNotNull { file ->
                file.name.removePrefix("segment_").toIntOrNull()
            }?.toSet() ?: emptySet()

            val allSegmentNames = (0 until totalSegments).toList()

            val missingSegmentNames = allSegmentNames.filterNot { it in existingSegments }

            return tempFolder to missingSegmentNames
        } else {            
            if (Constants.DLSEG) {
                Logger.info("Creating temporary folder $tempFolderName")
                println("\n")
                tempFolder.mkdirs()
            }            
        }
        return tempFolder to emptyList()
    }

    private fun generateRanges(size: Long, step: Long = FRAGMENT_SIZE_IN_BYTES): List<LongRange> {
        val ranges = mutableListOf<LongRange>()

        // if the size is less than or equal to step size return a single range
        if (size <= step) {
            ranges.add(0 until size)
            return ranges
        }

        var start = 0L
        while (start < size) {
            val end = minOf(start + step, size) // ensure the end doesn't exceed the size
            ranges.add(start until end)
            start = end
        }

        return ranges
    }


    private fun generateSegmentTokens(simpleVideo: SimpleVideo?): Map<Int, String> {
        Logger.debug("Generating segment request tokens.")
        val fragmentList = mutableMapOf<Int, String>()
        val encryptionKey = cryptoHelper.getKey(simpleVideo?.size)
        Logger.debug("encryptionKey : $encryptionKey")  
        if (simpleVideo?.size != null) {
            val ranges = generateRanges(simpleVideo.size)
            ranges.forEachIndexed { index, _ ->
                val path = "/mp4/${simpleVideo.md5_id}/${simpleVideo.resId}/${simpleVideo.size}/$FRAGMENT_SIZE_IN_BYTES/$index"
                    if (index == 0) {
                        Logger.debug("path : $path")
                        Logger.debug("md5_id : ${simpleVideo.md5_id}")
                        Logger.debug("resId : ${simpleVideo.resId}")
                        Logger.debug("size : ${simpleVideo.size}")
                        Logger.debug("fragment : $FRAGMENT_SIZE_IN_BYTES")
                        Logger.debug("index : $index")                                      
                    }
                
                val encryptedBody = cryptoHelper.encryptAESCTR(path, encryptionKey)
                    if (index == 0) {
                        Logger.debug("encryptedBody : $encryptedBody")
                    }
                    
                fragmentList[index] = doubleEncodeToBase64(encryptedBody)                
                    if (index == 0) {
                    val frag0 = fragmentList[0]
                    Logger.debug("fragmentList[0] : $frag0")
                    }                
            }
            Logger.debug("fragmentList.size : ${fragmentList.size} request token generated")
            return fragmentList
        }
        return emptyMap()
    }

    private fun doubleEncodeToBase64(input: String): String {
        val first = Base64.getEncoder()
            .encodeToString(input.toByteArray(Charsets.ISO_8859_1))
            .replace("=", "")

        return Base64.getEncoder()
            .encodeToString(first.toByteArray())
            .replace("=", "")
    }


    private suspend fun requestSegment(url: String, token: String, index: Int? = null): Flow<ByteArray> = flow {
        Logger.debug("[$index] Starting request to $url with token token: $token")
        val response = Unirest.get(url)
            .header("Referer", "https://abysscdn.com/")
            .asBinary()

        val rawBody = response.rawBody
        val responseCode = response.status

        Logger.debug("[$index] Received response with status $responseCode\n", responseCode !in 200..299)

        val buffer = ByteArray(65536)
        var bytesRead: Int

        rawBody.use { stream ->
            while (stream.read(buffer).also { bytesRead = it } != -1) {
                emit(buffer.copyOf(bytesRead))
            }
        }
    }.flowOn(Dispatchers.IO)

    // tanda ? pada String? output artinya boleh return null
    fun testCoeg(coeg: String): String? {
        //val datasC = "eyJzbHVnIjoiRWQzbWYtcjJ3IiwibWQ1X2lkIjozMDU2ODE0NCwidXNlcl9pZCI6NTA0MDU0LCJtZWRpYSI6IufoXHUwMDBimVfVMta9XHUwMDAwgzV3ck6d0lx1MDAwZqbZ1WYhcc2I812tjFx1MDAxNC+QK1x1MDAxOGJB0j7N2jJ98HZalWNcdTAwMDfKMrBcdTAwMDRD7+9Do5JcdTAwMTThv2ZzLLSQXHRnSyclbPucyZR/ze6d2adcdTAwMTZFd1x1MDAwNFx1MDAxMIpcdTAwMDAghFJbwWnSoj9qbMBlS5xcdTAwMTR6zI9cdTAwMTFLXHUwMDE2RXXQXHUwMDA0laBcdTAwMTIorGQ/++lcdTAwMDG/+t9cdTAwMWaZJtmjWlx1MDAwM8xY8YExoITu+zWx3JDCrsZ/msOHgtlT9P5RXHUwMDFka6phRdBcIr8upUhHzWkzeIRWzHBcdTAwMTnkPH/+slx1MDAxMdLYXHUwMDBlXHUwMDE2tJI9e9dZMSO/0+TsN09bb8FtXe/7d2Ft5JZcdTAwMTildeHlh8v9XHUwMDFifjDC6aZYLUo/0YSD2oNcdJWKskxcdTAwMWOe407GnivinN/Ejb2NLSM+mVwigGFcZnTd03/BNaihvaH3OoxdlVxyXiZ5JrbUo7OywXf/XHUwMDAzMVxyjUhcdTAwMTF9V5RcdTAwMTRvnYGz5nyNXHUwMDFkgL5cdTAwMWS9i0CHdqlcdTAwMGZyu+Tzclx1MDAxZbhcdTAwMTU63zDb46olnnGYQNd1gVx1MDAxY1x1MDAxZo4/XCKSd7X+XGKbq1x1MDAwMyDUXHJcYuDCwFvEXFyo60ufgNJiSMhcdTAwMDdcdTAwMTXHaJVcdTAwMTm02sv2o76/I1x1MDAwNFxmOKLHZWpd6ETHvUVcdTAwMWWzKFx1MDAxY55x5jEuXHUwMDFl81xmXHUwMDA21yAs0b85XHUwMDFhQfrto7Hlw/Rm/6zNXHUwMDAxY3uf84zTKqO9XFwo65xcbj8owVx1MDAxM/uJ8ZSrwvf/0HtrXHUwMDE3Xa8lMiu4nKJcYlx1MDAxM/ilNmZcdTAwMGVcZlL5tepshW5YJJlcdTAwMTW2h+/hX9u873nFwVi6kFZDk8lcdTAwMTQjXHUwMDExVFx1MDAxZUeryWGvf/JcYqI1XHUwMDE0Srv58Km96EFgXHUwMDA30f96XHUwMDE0Z0RpLsJe7ohcdTAwMTFXXHUwMDFkW0S3cTdJmoXHz1x1MDAwNVx1MDAwM7dNRaPX2zEhauRQMuiv5udcdTAwMGVcdTAwMTGAXHUwMDE3g5WbbVx1MDAxMVx1MDAwNyhMXHUwMDAwh71/sFNw7/59mlBq/2PqKaMmies1v+PqXG6eZU+LbFx0XHUwMDA1wJZcdTAwMTeXu3k2L1xcTlx1MDAwYsPaXHJcdTAwMWSfOFx1MDAxM9+EXHItTVxu+L+/mG9bXFzxwV9cdTAwMWTm+f1T+Pkz8jFjx07iiUi1TciDv1x1MDAxMIVcdTAwMDFp0lx1MDAxMMlTXHUwMDFjxWzYTuKBJFx1MDAwNEDOLY4sQVx1MDAxYeux4YGCcv9cdTAwMTV9OVx0XHUwMDFhSl9wx79UXHUwMDEwMvKl/rBcdTAwMDNcdTAwMTl/09w+XHUwMDFlkmc1QqJkiWKN0+eVXHUwMDE4ZWKmUnqHrVx1MDAxM3yT3c20T7/c99FcdTAwMDCFfrFcdTAwMWavJv9cdTAwMDYjXHUwMDFkXCK5KmG2I3F561x1MDAwN9lcdTAwMDHjtPknxdZlXHUwMDA0XHUwMDE1aFx1MDAwN/ttk/lpXHUwMDA2w1Illlx1MDAwYj+lXCJDufIuelvWxffZXHUwMDBmZZouyKO13OlcdTAwMWLHZFx1MDAwMjKlJaUlJP9yw5SgTTltJEFcdTAwMTdRQPB5QVx1MDAwNOzhV8gz+Fx1MDAwYsdOq4M9/vokTfRBO03eXHUwMDE2XHUwMDFldoo+Q/7wSVwizYhcdTAwMDebbVx1MDAwM1x1MDAxNdZcdHY+JCFcdTAwMTFcdTAwMDRcXKJKXHUwMDE2eFx1MDAxZYnIXHUwMDE2wCjnKzXFVICu4zjg0lTYQYkxhphcdTAwMDdDb9u4XHUwMDE2XHUwMDEyXHUwMDFkrP1/wERcdTAwMWa+V9dTdzm3NOKaq2dz1LVASSaVJJ2PRfVcdTAwMTSP/U1cdTAwMTV+80n5wEQwXHUwMDE2JLyWTEkk5lxmOKnQXHUwMDE08ufahS2JXHUwMDFjz7u5XHUwMDE2euF0ylx1MDAxNYdccjdfq85yUl28gNWEVMzUXHUwMDA3XHUwMDAw+NRcdTAwMDZNJDVH0Jn7m+JPpsZQglskmmOrXmd/XHUwMDE2zCDE9iizsIKVsXg8uK+kQiRSiFx0d9/Vo1x1MDAwMFx1MDAxOX4nXHRcdTAwMDdcdTAwMDN8kFx1MDAwMG8wz/nps+arrO5xmiWwiCYm0HyYs9krjDjLPZT4tcGOlkCofVx1MDAwNa1dXHUwMDA3xqhcZsuSy/F3sqa1it6uxZvBuG3tTTHCPqxcdTAwMGJXhyIsImNvbmZpZyI6eyJwb3N0ZXIiOmZhbHNlLCJwcmV2aWV3IjpmYWxzZSwiaXNEb3dubG9hZCI6dHJ1ZX0sImRhbm11Ijp7InZpZGVvSWQiOiJYa1lZMS1pb0tJVFM4NnJsV05zNTlRZVdpTVN2b3J4NHpEYTNJd2g5OGI4U0U3Nk5KMGRoWmdvQ0JwbkVXMTRyeERLOVN5c1Y3dUwwcHNLUTFwZGN2NEJEcjRTcmttTzFRNUE4N3BUQy1BIn19"
        
        // di Kotlin slash nya kebalik
        val fileReadC = File("D:/Delphi/A-TEST-BATVIDEO/Output/A/datasC.txt")        
            if (!fileReadC.exists()) {                 
            Logger.debug("fileReadC not found in current directory: ${fileReadC.absolutePath}")
            return null
            }

        // di Kotlin tidak boleh begini
        /*
            if !fileC.exists()) {  
            val asd = "asd" 
            Logger.debug("File Exist")            
            } else {
            Logger.debug("File NOT Exist") 
            return null
            }
        val zzz = asd        
        */
        // karena val asd ndak bakal kebaca sama zzz
        // jadi harus pake if not then exit

        val datasC = fileReadC.readText()
        Logger.debug("${datasC}")              
        val decodedDatasC = String(Base64.getDecoder().decode(datasC), Charsets.ISO_8859_1)
        Logger.debug("decodedDatasC : ${decodedDatasC}")
        val mediaMetadataC = decodedDatasC.toObject<Datas>()
        val encryptedMediaMetadataC = mediaMetadataC.media
        Logger.debug("encryptedMediaMetadataC : ${encryptedMediaMetadataC}")
        val encryptedMediaMetadataSizeC = encryptedMediaMetadataC?.length        
        Logger.debug("encryptedMediaMetadataSizeC : ${encryptedMediaMetadataSizeC}")
        
        if (encryptedMediaMetadataC == null) {
            Logger.error("failed to get encrypted mediaC")
            return null
        }
        
        val mediaKeyC = "${mediaMetadataC.user_id}:${mediaMetadataC.slug}:${mediaMetadataC.md5_id}"
        Logger.debug("mediaKeyC : ${mediaKeyC}")
        val decryptionKeySC = cryptoHelper.getKey(mediaKeyC)
        Logger.debug("decryptionKeySC : ${decryptionKeySC}")
        val decryptionKeyC = cryptoHelper.getKey(mediaKeyC).toByteArray()
        Logger.debug("decryptionKeyC : ${decryptionKeyC}")
        val mediaSources0C = cryptoHelper.decryptString(encryptedMediaMetadataC, decryptionKeyC)
        Logger.debug("mediaSources0C : ${mediaSources0C}")
        val mediaSourcesSize0C = mediaSources0C?.length
        Logger.debug("mediaSourcesSize0C : ${mediaSourcesSize0C}")

        val fileWriteC = File("D:/Delphi/A-TEST-BATVIDEO/Output/A/mediaSources0C.txt")
        fileWriteC.writeText("${mediaSources0C}")
                
        val returnValueC = "return Woy"
        
        return returnValueC
    }


}
