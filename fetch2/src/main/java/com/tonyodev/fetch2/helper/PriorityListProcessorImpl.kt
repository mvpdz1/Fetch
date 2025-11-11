package com.tonyodev.fetch2.helper

import android.annotation.SuppressLint
import com.tonyodev.fetch2.*
import com.tonyodev.fetch2.downloader.DownloadManager
import com.tonyodev.fetch2core.HandlerWrapper
import com.tonyodev.fetch2.provider.DownloadProvider
import com.tonyodev.fetch2.provider.NetworkInfoProvider
import com.tonyodev.fetch2.util.DEFAULT_PRIORITY_QUEUE_INTERVAL_IN_MILLISECONDS
import com.tonyodev.fetch2.NetworkType
import com.tonyodev.fetch2.fetch.ListenerCoordinator
import com.tonyodev.fetch2core.Logger
import com.tonyodev.fetch2core.isFetchFileServerUrl
import java.util.concurrent.TimeUnit

@SuppressLint("UnspecifiedRegisterReceiverFlag")
class PriorityListProcessorImpl(private val handlerWrapper: HandlerWrapper,
                                private val downloadProvider: DownloadProvider,
                                private val downloadManager: DownloadManager,
                                private val networkInfoProvider: NetworkInfoProvider,
                                private val logger: Logger,
                                private val listenerCoordinator: ListenerCoordinator,
                                @Volatile
                                            override var downloadConcurrentLimit: Int,
                                private val namespace: String,
                                private val prioritySort: PrioritySort
    )
    : PriorityListProcessor<Download>, PriorityBackoffResetCallback {

    private val lock = Any()
    @Volatile
    override var globalNetworkType = NetworkType.GLOBAL_OFF
    @Volatile
    private var paused = false
    override val isPaused: Boolean
        get() = paused
    @Volatile
    private var stopped = true
    override val isStopped: Boolean
        get() = stopped
    @Volatile
    private var backOffTime = DEFAULT_PRIORITY_QUEUE_INTERVAL_IN_MILLISECONDS
    private val networkChangeListener: NetworkInfoProvider.NetworkChangeListener = object : NetworkInfoProvider.NetworkChangeListener {
        override fun onNetworkChanged() {
            handlerWrapper.post {
                if (!stopped && !paused && networkInfoProvider.isNetworkAvailable
                        && backOffTime > DEFAULT_PRIORITY_QUEUE_INTERVAL_IN_MILLISECONDS) {
                    resetBackOffTime()
                }
            }
        }
    }

    init {
        networkInfoProvider.registerNetworkChangeListener(networkChangeListener)
    }

    private val priorityIteratorRunnable = Runnable {
        if (canContinueToProcess()) {
            val priorityList = getPriorityList()
            var hasPendingTasks = priorityList.isNotEmpty()
            var startedAnyDownload = false
            
            val canAccommodate = downloadManager.canAccommodateNewDownload()
            val isNetworkAvailable = networkInfoProvider.isNetworkAvailable
            val activeDownloadCount = downloadManager.getActiveDownloadCount()
            
            logger.d("PriorityIterator check: canAccommodate=$canAccommodate, isNetworkAvailable=$isNetworkAvailable, " +
                    "pendingTasks=${priorityList.size}, activeDownloads=$activeDownloadCount, backoffTime=${backOffTime}ms")
            
            if (canAccommodate && canContinueToProcess()) {
                var shouldBackOff = false
                // 核心问题修复：即使网络检查失败，也应该尝试启动下载（网络检查可能不准确）
                // 只有在确实没有任务时才backoff
                if (priorityList.isEmpty()) {
                    shouldBackOff = true
                    logger.d("PriorityIterator: no pending tasks, backing off")
                } else if (!isNetworkAvailable) {
                    // 网络检查失败，但仍有任务，记录日志但不立即backoff
                    // 允许尝试启动下载，让下载器自己处理网络错误
                    logger.d("PriorityIterator: network check failed but has ${priorityList.size} pending tasks, will try anyway")
                }
                
                if (!shouldBackOff) {
                    shouldBackOff = true
                    for (index in 0..priorityList.lastIndex) {
                        if (downloadManager.canAccommodateNewDownload() && canContinueToProcess()) {
                            val download = priorityList[index]
                            val isFetchServerRequest = isFetchFileServerUrl(download.url)
                            
                            // 核心修复：对于非Fetch服务器请求，即使网络检查失败也尝试启动
                            // 因为网络检查可能不准确，实际下载时网络可能是可用的
                            val shouldTryStart = isFetchServerRequest || isNetworkAvailable
                            
                            if (shouldTryStart && canContinueToProcess()) {
                                val networkType = when {
                                    globalNetworkType != NetworkType.GLOBAL_OFF -> globalNetworkType
                                    download.networkType == NetworkType.GLOBAL_OFF -> NetworkType.ALL
                                    else -> download.networkType
                                }
                                val properNetworkConditions = networkInfoProvider.isOnAllowedNetwork(networkType)
                                
                                logger.d("PriorityIterator: download ${download.id}, isFetchServer=$isFetchServerRequest, " +
                                        "networkType=$networkType, properNetworkConditions=$properNetworkConditions")
                                
                                if (!properNetworkConditions && !isFetchServerRequest) {
                                    listenerCoordinator.mainListener.onWaitingNetwork(download)
                                    logger.d("PriorityIterator: waiting for proper network conditions for download ${download.id}")
                                }
                                
                                // 核心修复：即使网络条件不完全满足，也尝试启动下载
                                // 让下载器在实际连接时处理网络错误，而不是在这里阻止
                                // 如果网络检查失败，仍然尝试启动（网络检查可能不准确）
                                if (isFetchServerRequest || properNetworkConditions) {
                                    shouldBackOff = false
                                    if (!downloadManager.contains(download.id) && canContinueToProcess()) {
                                        logger.d("PriorityIterator: attempting to start download ${download.id}")
                                        if (downloadManager.start(download)) {
                                            startedAnyDownload = true
                                            logger.d("PriorityIterator: successfully started download ${download.id}")
                                        } else {
                                            logger.d("PriorityIterator: failed to start download ${download.id}")
                                        }
                                    } else {
                                        logger.d("PriorityIterator: download ${download.id} already in progress or manager doesn't contain it")
                                    }
                                }
                            } else {
                                logger.d("PriorityIterator: skipping download ${download.id}, shouldTryStart=$shouldTryStart")
                                // 不要break，继续尝试其他任务
                            }
                        } else {
                            logger.d("PriorityIterator: cannot accommodate more downloads, breaking loop")
                            break
                        }
                    }
                }
                
                if (shouldBackOff) {
                    logger.d("PriorityIterator: backing off, hasPendingTasks=$hasPendingTasks")
                    increaseBackOffTime(hasPendingTasks)
                } else if (startedAnyDownload) {
                    // 成功启动下载时重置backoff时间
                    logger.d("PriorityIterator: resetting backoff time after starting download")
                    resetBackOffTime()
                } else if (hasPendingTasks) {
                    // 有任务但没有启动，可能是网络条件不满足，但不要无限增长backoff
                    logger.d("PriorityIterator: has pending tasks but didn't start any, limiting backoff")
                    if (backOffTime > MAX_BACKOFF_TIME_WHEN_PENDING) {
                        backOffTime = MAX_BACKOFF_TIME_WHEN_PENDING
                    }
                }
            } else if (hasPendingTasks) {
                // 即使无法启动新任务，如果有待处理任务，立即重置为较短的检查间隔
                // 确保任务能尽快开始，而不是等待很长时间
                if (backOffTime > MAX_BACKOFF_TIME_WHEN_PENDING) {
                    backOffTime = MAX_BACKOFF_TIME_WHEN_PENDING
                    val seconds = TimeUnit.MILLISECONDS.toSeconds(backOffTime)
                    logger.d("PriorityIterator backoffTime reset to $seconds seconds due to pending tasks")
                } else {
                    // 如果有待处理任务但无法启动，保持当前backoff时间或稍微增加，但不超过上限
                    // 这样可以避免频繁检查，同时确保任务能尽快开始
                    backOffTime = minOf(
                        maxOf(backOffTime, DEFAULT_PRIORITY_QUEUE_INTERVAL_IN_MILLISECONDS),
                        MAX_BACKOFF_TIME_WHEN_PENDING
                    )
                }
            }
            if (canContinueToProcess()) {
                registerPriorityIterator()
            }
        }
    }

    override fun start() {
        synchronized(lock) {
            resetBackOffTime()
            stopped = false
            paused = false
            registerPriorityIterator()
            logger.d("PriorityIterator started")
        }
    }

    override fun stop() {
        synchronized(lock) {
            unregisterPriorityIterator()
            paused = false
            stopped = true
            downloadManager.cancelAll()
            logger.d("PriorityIterator stop")
        }
    }

    override fun pause() {
        synchronized(lock) {
            unregisterPriorityIterator()
            paused = true
            stopped = false
            downloadManager.cancelAll()
            logger.d("PriorityIterator paused")
        }
    }

    override fun resume() {
        synchronized(lock) {
            resetBackOffTime()
            paused = false
            stopped = false
            registerPriorityIterator()
            logger.d("PriorityIterator resumed")
        }
    }

    override fun getPriorityList(): List<Download> {
        synchronized(lock) {
            return try {
                downloadProvider.getPendingDownloadsSorted(prioritySort)
            } catch (e: Exception) {
                logger.d("PriorityIterator failed access database", e)
                listOf()
            }
        }
    }

    override fun onResetBackoffTime(namespace: String?) {
        handlerWrapper.post {
            if (!stopped && !paused && this.namespace == namespace) {
                resetBackOffTime()
            }
        }
    }


    private fun registerPriorityIterator() {
        if (downloadConcurrentLimit > 0) {
            handlerWrapper.postDelayed(priorityIteratorRunnable, backOffTime)
        }
    }

    private fun unregisterPriorityIterator() {
        if (downloadConcurrentLimit > 0) {
            handlerWrapper.removeCallbacks(priorityIteratorRunnable)
        }
    }

    private fun canContinueToProcess(): Boolean {
        return !stopped && !paused
    }

    override fun resetBackOffTime() {
        synchronized(lock) {
            backOffTime = DEFAULT_PRIORITY_QUEUE_INTERVAL_IN_MILLISECONDS
            unregisterPriorityIterator()
            registerPriorityIterator()
            logger.d("PriorityIterator backoffTime reset to $backOffTime milliseconds")
        }
    }

    override fun sendBackOffResetSignal() {
        synchronized(lock) {
            onResetBackoffTime(namespace)
        }
    }

    override fun close() {
        synchronized(lock) {
            networkInfoProvider.unregisterNetworkChangeListener(networkChangeListener)
        }
    }

    private fun increaseBackOffTime(hasPendingTasks: Boolean = false) {
        if (hasPendingTasks) {
            // 如果有待处理任务，限制backoff时间的增长，最多30秒
            backOffTime = if (backOffTime == DEFAULT_PRIORITY_QUEUE_INTERVAL_IN_MILLISECONDS) {
                MAX_BACKOFF_TIME_WHEN_PENDING
            } else {
                minOf(backOffTime * 2L, MAX_BACKOFF_TIME_WHEN_PENDING)
            }
            val seconds = TimeUnit.MILLISECONDS.toSeconds(backOffTime)
            logger.d("PriorityIterator backoffTime increased to $seconds seconds (pending tasks)")
        } else {
            // 没有待处理任务时，可以增长更长时间
            backOffTime = if (backOffTime == DEFAULT_PRIORITY_QUEUE_INTERVAL_IN_MILLISECONDS) {
                ONE_MINUTE_IN_MILLISECONDS
            } else {
                backOffTime * 2L
            }
            val minutes = TimeUnit.MILLISECONDS.toMinutes(backOffTime)
            logger.d("PriorityIterator backoffTime increased to $minutes minute(s)")
        }
    }

    private companion object {
        private const val ONE_MINUTE_IN_MILLISECONDS = 60000L
        // 当有待处理任务时，backoff时间上限为30秒，确保任务能尽快开始
        private const val MAX_BACKOFF_TIME_WHEN_PENDING = 30000L // 30秒
    }

}
