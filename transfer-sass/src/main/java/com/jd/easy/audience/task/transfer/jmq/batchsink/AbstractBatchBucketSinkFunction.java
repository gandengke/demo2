package com.jd.easy.audience.task.transfer.jmq.batchsink;

import com.jd.bdp.rrd.apus.flink.sdk.aync2.retry.RetryPolicy;
import com.jd.bdp.rrd.apus.flink.sdk.batchsink2.BatchSinkConfig;
import com.jd.bdp.rrd.apus.flink.sdk.batchsink2.IBatchProcessCallBack;
import com.jd.bdp.rrd.apus.flink.sdk.batchsink2.IBatchSinkProcess;
import com.jd.bdp.rrd.apus.flink.sdk.batchsink2.PendingWaitStrategy;
import com.jd.bdp.rrd.apus.flink.sdk.common.ExecutorUtils;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.shaded.guava18.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.fs.Clock;
import org.apache.flink.streaming.connectors.fs.StringWriter;
import org.apache.flink.streaming.connectors.fs.Writer;
import org.apache.flink.streaming.connectors.fs.bucketing.Bucketer;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 自定义批量写入bucket
 *
 * @author cdxiongmei
 * @version V1.0
 */
public class AbstractBatchBucketSinkFunction extends RichSinkFunction<String> implements CheckpointedFunction, CheckpointListener, IBatchSinkProcess<String>, ProcessingTimeCallback {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractBatchBucketSinkFunction.class);

    private String basePath;
    private Bucketer<String> bucketer;
    private transient Clock clock;
    private transient State<String> state;
    private transient ListState<State<String>> restoredBucketStates;
    /**
     * 检查桶活跃的间隔时间（ms）-一分钟检查一次
     */
    private long inactiveBucketCheckInterval = 60000L;
    /**
     * 桶活跃度超时时长-1分钟
     */
    private long inactiveBucketThreshold = 60000L;
    // TODO
    private long batchRolloverInterval = 9223372036854775807L;
    //    private DataWrteCommon dataWrteCommon;
    private transient ProcessingTimeService processingTimeService;
    private static final long DEFAULT_BATCH_SIZE = 1024L * 1024L * 384L;
    private String inProgressSuffix = ".in-progress";
    private String inProgressPrefix = "_";
    private String pendingSuffix = ".pending";
    private String pendingPrefix = "_";
    private String partPrefix = "part";
    private String partSuffix = null;
    private String validLengthSuffix = ".valid-length";
    private String validLengthPrefix = "_";
    private transient FileSystem fs;
    private transient Method refTruncate;
    private Writer<List<String>> writerTemplate;
    @Setter
    private boolean useTruncate = true;
    private long asyncTimeout = 60 * 1000L;
    //batch
    @Setter
    protected BatchSinkConfig conf;
    @Nullable
    private Configuration fsConfig;
    protected transient ThreadPoolExecutor batchProcessExecutor;
    private final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public AbstractBatchBucketSinkFunction(@NonNull BatchSinkConfig batchSinkConfig, String basePath, Configuration configuration) {
        this(batchSinkConfig, null, null, basePath, configuration);
    }

    public AbstractBatchBucketSinkFunction(@NonNull BatchSinkConfig batchSinkConfig, RetryPolicy retryPolicy, PendingWaitStrategy waitStrategy, String basePath,  Configuration configuration) {
        Validate.notNull(batchSinkConfig);
        //批处理的间隔时间
        Validate.isTrue(batchSinkConfig.getBatchLingerMs() > 0);
        //并发线程数
        Validate.isTrue(batchSinkConfig.getBatchProcessorThreadNum() >= 0);
        //每个批次的数据量
        Validate.isTrue(batchSinkConfig.getBatchSize() >= 0);
        //能缓存的最大数据量
        int threadFullloadtotal = (batchSinkConfig.getBatchProcessorThreadNum() == 0 ? 1 : batchSinkConfig.getBatchProcessorThreadNum()) * batchSinkConfig.getBatchSize();
        if (batchSinkConfig.getProcessingTotalNum() < threadFullloadtotal) {
            batchSinkConfig.setProcessingTotalNum(threadFullloadtotal);
        }
        this.conf = batchSinkConfig;
//        this.retryPolicy = null == retryPolicy ? new NeverRetryPolicy() : retryPolicy;
//        this.waitStrategy = null == waitStrategy ? new BlockingPendingWaitStrategy() : waitStrategy;
        //bucketing
        this.basePath = basePath;
        this.bucketer = new DateTimeBucketer();
        this.writerTemplate = new StringWriter<List<String>>();
        fsConfig = configuration;
    }

    public AbstractBatchBucketSinkFunction setBucketer(Bucketer<String> bucketer) {
        this.bucketer = bucketer;
        return this;
    }

    @Override
    public void onProcessingTime(long l) throws Exception {
        long currentProcessingTime = this.processingTimeService.getCurrentProcessingTime();
        this.closePartFilesByTime(currentProcessingTime);
        this.processingTimeService.registerTimer(currentProcessingTime + this.inactiveBucketCheckInterval, this);

    }

    private void closePartFilesByTime(long currentProcessingTime) throws Exception {
//        LOG.info("TIME TO subTask ={} DELETE CURRENT FILE TIME ={}", getRuntimeContext().getIndexOfThisSubtask(), simpleDateFormat.format(new Date(currentProcessingTime)));
        synchronized (this.state.bucketStates) {
            for (Map.Entry<String, BucketState> entry : state.bucketStates.entrySet()) {
                if ((entry.getValue().lastWrittenToTime < currentProcessingTime - inactiveBucketThreshold)
                        || (entry.getValue().creationTime < currentProcessingTime - batchRolloverInterval)) {
                    LOG.debug("BucketingSink {} closing bucket due to inactivity of over {} ms.",
                            getRuntimeContext().getIndexOfThisSubtask(), inactiveBucketThreshold);
//                    LOG.info("TIME TO subTask ={} closeCurrentPartFile inactivity of over {} ms", getRuntimeContext().getIndexOfThisSubtask(), inactiveBucketThreshold);
                    closeCurrentPartFile(entry.getValue());
                }
            }

        }
    }

    private void closeCurrentPartFile(BucketState bucketState) throws Exception {
        if (bucketState.isWriterOpen) {
            bucketState.writer.close();
            bucketState.isWriterOpen = false;
//            bucketState.timerService.shutdownNow();
//            ExecutorUtils.safeShutdown(bucketState.batchProcessExecutor);
//            closeResource();
        }

        if (bucketState.currentFile != null) {
            Path currentPartPath = new Path(bucketState.currentFile);
            Path inProgressPath = this.getInProgressPathFor(currentPartPath);
            Path pendingPath = this.getPendingPathFor(currentPartPath);
            this.fs.rename(inProgressPath, pendingPath);
            LOG.debug("Moving in-progress bucket {} to pending file {}", inProgressPath, pendingPath);
            bucketState.pendingFiles.add(currentPartPath.toString());
            bucketState.currentFile = null;
        }

    }

    private Path getPendingPathFor(Path path) {
        return (new Path(path.getParent(), this.pendingPrefix + path.getName())).suffix(this.pendingSuffix);
    }

    private Path getInProgressPathFor(Path path) {
        return (new Path(path.getParent(), this.inProgressPrefix + path.getName())).suffix(this.inProgressSuffix);
    }

    private Path getValidLengthPathFor(Path path) {
        return new Path(path.getParent(), validLengthPrefix + path.getName()).suffix(validLengthSuffix);
    }

    @Override
    public void process(List<String> list, IBatchProcessCallBack iBatchProcessCallBack) {
        LOG.info("having writer " + list.toString());
        iBatchProcessCallBack.complete();

    }

    @Override
    public void initResource(Configuration configuration, RuntimeContext runtimeContext) throws Exception {

    }

    @Override
    public void closeResource() throws Exception {
//        dataWrteCommon.close();
    }
    @Override
    public void invoke(String value, Context context) throws Exception {
        if (StringUtils.isBlank(value)) {
            return;
        }
        handleBatchElement(value);
    }

    private Path assemblePartPath(Path bucket, int subtaskIndex, int partIndex) {
        String localPartSuffix = partSuffix != null ? partSuffix : "";
        return new Path(bucket, String.format("%s-%s-%s%s", partPrefix, subtaskIndex, partIndex, localPartSuffix));
    }

    private void openNewPartFile(Path bucketPath, BucketState bucketState) throws Exception {
        LOG.info("TIME TO subTask ={} ready to openNewPartFile bucketPath={}", getRuntimeContext().getIndexOfThisSubtask(), bucketState.bucketPath);
        closeCurrentPartFile(bucketState);
        if (!fs.exists(bucketPath)) {
            try {
                if (fs.mkdirs(bucketPath)) {
                    LOG.debug("Created new bucket directory: {}", bucketPath);
//                    LOG.info("TIME TO subTask ={} ready to create directory bucketPath={}", getRuntimeContext().getIndexOfThisSubtask(), bucketState.bucketPath);

                }
            } catch (IOException e) {
                throw new RuntimeException("Could not create new bucket path.", e);
            }
        }

        // The following loop tries different partCounter values in ascending order until it reaches the minimum
        // that is not yet used. This works since there is only one parallel subtask that tries names with this
        // subtask id. Otherwise we would run into concurrency issues here. This is aligned with the way we now
        // clean the base directory in case of rescaling.

        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        Path partPath = assemblePartPath(bucketPath, subtaskIndex, bucketState.partCounter);
        while (fs.exists(partPath) ||
                fs.exists(getPendingPathFor(partPath)) ||
                fs.exists(getInProgressPathFor(partPath))) {
            bucketState.partCounter++;
            partPath = assemblePartPath(bucketPath, subtaskIndex, bucketState.partCounter);
        }

        // Record the creation time of the bucket
        bucketState.creationTime = processingTimeService.getCurrentProcessingTime();

        // increase, so we don't have to check for this name next time
        bucketState.partCounter++;

        LOG.debug("Next part path is {}", partPath.toString());
        bucketState.currentFile = partPath.toString();

        Path inProgressPath = getInProgressPathFor(partPath);
        if (bucketState.writer == null) {
            bucketState.writer = writerTemplate.duplicate();
            if (bucketState.writer == null) {
                throw new UnsupportedOperationException(
                        "Could not duplicate writer. " +
                                "Class '" + writerTemplate.getClass().getCanonicalName() + "' must implement the 'Writer.duplicate()' method."
                );
            }
        }
        bucketState.writer.open(fs, inProgressPath);
        bucketState.isWriterOpen = true;
    }

    private void handleBatchElement(String element) throws Exception {
        LOG.info("TIME TO subTask ={} handleBatchElement element={}, state={}", getRuntimeContext().getIndexOfThisSubtask(), element, this.state.bucketStates.keySet().toString());
        Path bucketPath = this.bucketer.getBucketPath(this.clock, new Path(this.basePath), element);
        LOG.info("TIME TO subTask ={} handleBatchElement bucketPath={}", getRuntimeContext().getIndexOfThisSubtask(), bucketPath.toString());
        long currentProcessingTime = this.processingTimeService.getCurrentProcessingTime();
        BucketState bucketState = this.state.getBucketState(bucketPath);
        if (bucketState == null) {
//            LOG.info("TIME TO subTask ={} createBucket bucketPath={}", getRuntimeContext().getIndexOfThisSubtask(), bucketPath.toString());
            bucketState = new BucketState(bucketPath.toString(), conf);
            bucketState.resetBatch(currentProcessingTime, conf.getBatchSize(), conf.getBatchLingerMs(), this);
            this.state.addBucketState(bucketPath, bucketState);
//            LOG.info("TIME TO subTask ={} handleBatchElement after create state={}", getRuntimeContext().getIndexOfThisSubtask(), this.state.bucketStates.keySet().toString());

        }
        if (this.shouldRoll(bucketState, currentProcessingTime)) {
            //TODO
            LOG.info("TIME TO subTask ={} openNewPartFile bucketPath={}", getRuntimeContext().getIndexOfThisSubtask(), bucketPath);
            this.openNewPartFile(bucketPath, bucketState);
        }
        synchronized (bucketState) {
            if (bucketState.currentBatchCollection.batchReady.get()) {
//                LOG.info("TIME TO subTask ={} startnewBatch bucketPath={}", getRuntimeContext().getIndexOfThisSubtask(), bucketPath);
                bucketState.resetBatch(currentProcessingTime, conf.getBatchSize(), conf.getBatchLingerMs(), this);
            }
//            LOG.info("TIME TO subTask ={} addElement bucketPath={}", getRuntimeContext().getIndexOfThisSubtask(), bucketPath);
            bucketState.currentBatchCollection.add(element);
//            LOG.info("TIME TO subTask ={} addElement bucketPath={}, batchId={}, batchCount={}", getRuntimeContext().getIndexOfThisSubtask(), bucketPath, bucketState.currentBatchCollection.batchId, bucketState.currentBatchCollection.batchCount);
            if (bucketState.currentBatchCollection.isBatchFull()) {
//                LOG.info("TIME TO subTask ={} batchFull bucketPath={}, currentFile={}", getRuntimeContext().getIndexOfThisSubtask(), bucketPath, bucketState.currentFile);
                if (bucketState.currentBatchCollection.batchReady.compareAndSet(false, true)) {
                    bucketState.currentBatchCollection.cancelLingerTimer();
//                    LOG.info("TIME TO subTask ={} batchFull-processBatch currentProcessTime={}", getRuntimeContext().getIndexOfThisSubtask(), simpleDateFormat.format(new Date(currentProcessingTime)));
                    bucketState.processBatch(currentProcessingTime, this);
                }
            }

        }

    }

    /**
     * 判断是否需要分块
     *
     * @param bucketState
     * @param currentProcessingTime
     * @return
     * @throws IOException
     */
    private boolean shouldRoll(BucketState bucketState, long currentProcessingTime) throws IOException {
        boolean shouldRoll = false;
        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        if (!bucketState.isWriterOpen) {
            //TODO
            shouldRoll = true;
            LOG.info("BucketingSink {} starting new bucket.", subtaskIndex);
            LOG.info("TIME TO subTask ={} shouldRoll bucketPath={}", subtaskIndex, bucketState.bucketPath);
        } else {

            long writePosition = bucketState.writer.getPos();
            LOG.info("TIME TO subTask ={} shouldRoll bucketPath={}, pos={}", subtaskIndex, bucketState.bucketPath, writePosition);
            if (writePosition > DEFAULT_BATCH_SIZE) {
                //文件大小是否超过
                shouldRoll = true;
//                LOG.info(
//                        "BucketingSink {} starting new bucket because file position {} is above batch size {}.",
//                        subtaskIndex,
//                        writePosition,
//                        DEFAULT_BATCH_SIZE);
                LOG.info("TIME TO subTask ={} shouldRoll bucketPath={}, pos={}, above batch size {}", subtaskIndex, bucketState.bucketPath, writePosition, DEFAULT_BATCH_SIZE);
            } else {
                //文件创建时间
                LOG.info("TIME TO subTask ={} shouldRoll bucketPath={}, creationTime={}", subtaskIndex, bucketState.bucketPath, bucketState.creationTime);
                if ((currentProcessingTime - bucketState.creationTime) > batchRolloverInterval) {
                    shouldRoll = true;
                    LOG.info("TIME TO subTask ={} shouldRoll bucketPath={}, creationTime={} over interval{}", subtaskIndex, bucketState.bucketPath, bucketState.creationTime, batchRolloverInterval);
//                    LOG.info(
//                            "BucketingSink {} starting new bucket because file is older than roll over interval {}.",
//                            subtaskIndex,
//                            batchRolloverInterval);
                }
            }
        }
        return shouldRoll;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        if (conf.getBatchProcessorThreadNum() > 0) {
            this.batchProcessExecutor = new ThreadPoolExecutor(conf.getBatchProcessorThreadNum(), conf.getBatchProcessorThreadNum(),
                    0, TimeUnit.MILLISECONDS, new PriorityBlockingQueue<>(), new ThreadFactoryBuilder().setNameFormat("batchsink-processor-%d").build());

            initResource(parameters, getRuntimeContext());
            //bucketing
            this.state = new State();
            this.processingTimeService = ((StreamingRuntimeContext) this.getRuntimeContext()).getProcessingTimeService();
            long currentProcessingTime = this.processingTimeService.getCurrentProcessingTime();
            //sink初始化的时候注册定时器
            this.processingTimeService.registerTimer(currentProcessingTime + this.inactiveBucketCheckInterval, this);
            this.clock = new Clock() {
                @Override
                public long currentTimeMillis() {
                    return AbstractBatchBucketSinkFunction.this.processingTimeService.getCurrentProcessingTime();
                }
            };
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        ExecutorUtils.safeShutdown(batchProcessExecutor);
        //bucketing
        if (state != null) {
            for (Map.Entry<String, AbstractBatchBucketSinkFunction.BucketState> entry : state.bucketStates.entrySet()) {
                closeCurrentPartFile(entry.getValue());
                entry.getValue().timerService.shutdownNow();
                ExecutorUtils.safeShutdown(batchProcessExecutor);
            }
        }
        closeResource();
    }

    /**
     * Create a file system with the user-defined {@code HDFS} configuration.
     *
     * @throws IOException
     */
    private void initFileSystem() throws IOException {
        if (fs == null) {
            Path path = new Path(basePath);
            fs = createHadoopFileSystem(path, fsConfig);
        }
    }

    public static FileSystem createHadoopFileSystem(
            Path path,
            @Nullable Configuration extraUserConf) throws IOException {

        // try to get the Hadoop File System via the Flink File Systems
        // that way we get the proper configuration

        final org.apache.flink.core.fs.FileSystem flinkFs =
                org.apache.flink.core.fs.FileSystem.getUnguardedFileSystem(path.toUri());
        final FileSystem hadoopFs = (flinkFs instanceof HadoopFileSystem) ?
                ((HadoopFileSystem) flinkFs).getHadoopFileSystem() : null;

        // fast path: if the Flink file system wraps Hadoop anyways and we need no extra config,
        // then we use it directly
        if (extraUserConf == null && hadoopFs != null) {
            return hadoopFs;
        } else {
            // we need to re-instantiate the Hadoop file system, because we either have
            // a special config, or the Path gave us a Flink FS that is not backed by
            // Hadoop (like file://)

            final org.apache.hadoop.conf.Configuration hadoopConf;
            if (hadoopFs != null) {
                // have a Hadoop FS but need to apply extra config
                hadoopConf = hadoopFs.getConf();
            } else {
                // the Path gave us a Flink FS that is not backed by Hadoop (like file://)
                // we need to get access to the Hadoop file system first

                // we access the Hadoop FS in Flink, which carries the proper
                // Hadoop configuration. we should get rid of this once the bucketing sink is
                // properly implemented against Flink's FS abstraction

                URI genericHdfsUri = URI.create("hdfs://localhost:12345/");
                org.apache.flink.core.fs.FileSystem accessor =
                        org.apache.flink.core.fs.FileSystem.getUnguardedFileSystem(genericHdfsUri);

                if (!(accessor instanceof HadoopFileSystem)) {
                    throw new IOException(
                            "Cannot instantiate a Hadoop file system to access the Hadoop configuration. " +
                                    "FS for hdfs:// is " + accessor.getClass().getName());
                }

                hadoopConf = ((HadoopFileSystem) accessor).getHadoopFileSystem().getConf();
            }

            // finalize the configuration

            final org.apache.hadoop.conf.Configuration finalConf;
            if (extraUserConf == null) {
                finalConf = hadoopConf;
            } else {
                finalConf = new org.apache.hadoop.conf.Configuration(hadoopConf);

                for (String key : extraUserConf.keySet()) {
                    finalConf.set(key, extraUserConf.getString(key, null));
                }
            }

            // we explicitly re-instantiate the file system here in order to make sure
            // that the configuration is applied.

            URI fsUri = path.toUri();
            final String scheme = fsUri.getScheme();
            final String authority = fsUri.getAuthority();

            if (scheme == null && authority == null) {
                fsUri = FileSystem.getDefaultUri(finalConf);
            } else if (scheme != null && authority == null) {
                URI defaultUri = FileSystem.getDefaultUri(finalConf);
                if (scheme.equals(defaultUri.getScheme()) && defaultUri.getAuthority() != null) {
                    fsUri = defaultUri;
                }
            }

            final Class<? extends FileSystem> fsClass = FileSystem.getFileSystemClass(fsUri.getScheme(), finalConf);
            final FileSystem fs;
            try {
                fs = fsClass.newInstance();
            } catch (Exception e) {
                throw new IOException("Cannot instantiate the Hadoop file system", e);
            }

            fs.initialize(fsUri, finalConf);

            // We don't perform checksums on Hadoop's local filesystem and use the raw filesystem.
            // Otherwise buffers are not flushed entirely during checkpointing which results in data loss.
            if (fs instanceof LocalFileSystem) {
                return ((LocalFileSystem) fs).getRaw();
            }
            return fs;
        }
    }

    private Method reflectTruncate(FileSystem fs) {
        // completely disable the check for truncate() because the check can be problematic
        // on some filesystem implementations
        if (!useTruncate) {
            return null;
        }

        Method m = null;
        if (fs != null) {
            Class<?> fsClass = fs.getClass();
            try {
                m = fsClass.getMethod("truncate", Path.class, long.class);
            } catch (NoSuchMethodException ex) {
                LOG.debug("Truncate not found. Will write a file with suffix '{}' " +
                        " and prefix '{}' to specify how many bytes in a bucket are valid.", validLengthSuffix, validLengthPrefix);
                return null;
            }

            // verify that truncate actually works
            Path testPath = new Path(basePath, UUID.randomUUID().toString());
            LOG.info("testPath=" + testPath.toString());
            try {
                try (FSDataOutputStream outputStream = fs.create(testPath)) {
                    outputStream.writeUTF("hello");
                } catch (IOException e) {
                    LOG.error("Could not create file for checking if truncate works.", e);
                    throw new RuntimeException(
                            "Could not create file for checking if truncate works. " +
                                    "You can disable support for truncate() completely via " +
                                    "BucketingSink.setUseTruncate(false).", e);
                }

                try {
                    m.invoke(fs, testPath, 2);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    LOG.debug("Truncate is not supported.", e);
                    m = null;
                }
            } finally {
                try {
                    fs.delete(testPath, false);
                } catch (IOException e) {
                    LOG.error("Could not delete truncate test file.", e);
                    throw new RuntimeException("Could not delete truncate test file. " +
                            "You can disable support for truncate() completely via " +
                            "BucketingSink.setUseTruncate(false).", e);
                }
            }
        }
        return m;
    }

    /**
     * 每次快找时保存所有数据到文件中，并进行文件状态的更新
     *
     * @param context
     * @throws Exception
     */
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        LOG.info("TIME TO subTask ={} snapshotState TIME ={}", getRuntimeContext().getIndexOfThisSubtask(), simpleDateFormat.format(new Date()));
        checkNotNull(restoredBucketStates);
        restoredBucketStates.clear();
        synchronized (state.bucketStates) {
            int subtaskIdx = getRuntimeContext().getIndexOfThisSubtask();
            for (Map.Entry<String, BucketState> bucketStateEntry : state.bucketStates.entrySet()) {
                BucketState bucketState = bucketStateEntry.getValue();
                if (bucketState.currentBatchCollection.batchReady.compareAndSet(false, true)) {
                    if (!bucketState.isWriterOpen) {
                        openNewPartFile(new Path(bucketStateEntry.getKey()), bucketState);
                    }
                    LOG.info("TIME TO subTask ={} snapshotState-processBatch TIME ={}", getRuntimeContext().getIndexOfThisSubtask(), simpleDateFormat.format(new Date()));
                    bucketState.currentBatchCollection.cancelLingerTimer();
                    bucketState.processBatch(processingTimeService.getCurrentProcessingTime(), this);
                    bucketState.resetBatch(processingTimeService.getCurrentProcessingTime(), conf.getBatchSize(), conf.getBatchLingerMs(), this);
                }
                if (bucketState.isWriterOpen) {
                    bucketState.currentFileValidLength = bucketState.writer.flush();
                }
                synchronized (bucketState.pendingFilesPerCheckpoint) {
                    bucketState.pendingFilesPerCheckpoint.put(context.getCheckpointId(), bucketState.pendingFiles);
                }
                bucketState.pendingFiles = new ArrayList<>();
            }
            restoredBucketStates.add(state);

            if (LOG.isDebugEnabled()) {
                LOG.debug("{} idx {} checkpointed {}.", getClass().getSimpleName(), subtaskIdx, state);
            }
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        Preconditions.checkArgument(this.restoredBucketStates == null, "The operator has already been initialized.");
        LOG.info("TIME TO subTask ={} initializeState TIME ={}", getRuntimeContext().getIndexOfThisSubtask(), simpleDateFormat.format(new Date()));

        try {
            initFileSystem();
        } catch (IOException e) {
            LOG.error("Error while creating FileSystem when initializing the state of the BucketingSink.", e);
            throw new RuntimeException("Error while creating FileSystem when initializing the state of the BucketingSink.", e);
        }
        //truncate方法
        if (this.refTruncate == null) {
            this.refTruncate = reflectTruncate(fs);
        }

        OperatorStateStore stateStore = context.getOperatorStateStore();
        restoredBucketStates = stateStore.getSerializableListState("bucket-states");

        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        if (context.isRestored()) {
            LOG.info("Restoring state for the {} (taskIdx={}).", getClass().getSimpleName(), subtaskIndex);

            for (State<String> recoveredState : restoredBucketStates.get()) {
                //c从上次checcheckpoint
                LOG.info("TIME TO subTask ={} restoredBucketStates.get() state ={}", getRuntimeContext().getIndexOfThisSubtask(), restoredBucketStates.get().toString());

                handleRestoredBucketState(recoveredState);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("{} idx {} restored {}", getClass().getSimpleName(), subtaskIndex, recoveredState);
                }
            }
        } else {
            LOG.info("No state to restore for the {} (taskIdx={}).", getClass().getSimpleName(), subtaskIndex);
        }
    }

    private void handleRestoredBucketState(State<String> restoredState) {
        checkNotNull(restoredState);
        for (BucketState bucketState : restoredState.bucketStates.values()) {
            LOG.info("{} ready to restore,currentFile={}", bucketState.bucketPath, bucketState.currentFile);
            bucketState.pendingFiles.clear();
            handlePendingInProgressFile(bucketState.currentFile, bucketState.currentFileValidLength);

            // Now that we've restored the bucket to a valid state, reset the current file info
            bucketState.currentFile = null;
            bucketState.currentFileValidLength = -1;
            bucketState.isWriterOpen = false;

            handlePendingFilesForPreviousCheckpoints(bucketState.pendingFilesPerCheckpoint);

            bucketState.pendingFilesPerCheckpoint.clear();
        }
    }
    public static <T> T checkNotNull(T reference) {
        if (reference == null) {
            throw new NullPointerException();
        } else {
            return reference;
        }
    }
    private void handlePendingInProgressFile(String file, long validLength) {
        if (file != null) {

            // We were writing to a file when the last checkpoint occurred. This file can either
            // be still in-progress or became a pending file at some point after the checkpoint.
            // Either way, we have to truncate it back to a valid state (or write a .valid-length
            // file that specifies up to which length it is valid) and rename it to the final name
            // before starting a new bucket file.

            Path partPath = new Path(file);
            try {
                Path partPendingPath = getPendingPathFor(partPath);
                Path partInProgressPath = getInProgressPathFor(partPath);

                if (fs.exists(partPendingPath)) {
                    LOG.debug("In-progress file {} has been moved to pending after checkpoint, moving to final location.", partPath);
                    // has been moved to pending in the mean time, rename to final location
                    fs.rename(partPendingPath, partPath);
                } else if (fs.exists(partInProgressPath)) {
                    LOG.debug("In-progress file {} is still in-progress, moving to final location.", partPath);
                    // it was still in progress, rename to final path
                    fs.rename(partInProgressPath, partPath);
                } else if (fs.exists(partPath)) {
                    LOG.debug("In-Progress file {} was already moved to final location {}.", file, partPath);
                } else {
                    LOG.debug("In-Progress file {} was neither moved to pending nor is still in progress. Possibly, " +
                            "it was moved to final location by a previous snapshot restore", file);
                }

                // We use reflection to get the .truncate() method, this
                // is only available starting with Hadoop 2.7
                if (this.refTruncate == null) {
                    this.refTruncate = reflectTruncate(fs);
                }

                // truncate it or write a ".valid-length" file to specify up to which point it is valid
                if (refTruncate != null) {
                    LOG.debug("Truncating {} to valid length {}", partPath, validLength);
                    // some-one else might still hold the lease from a previous try, we are
                    // recovering, after all ...
                    if (fs instanceof DistributedFileSystem) {
                        DistributedFileSystem dfs = (DistributedFileSystem) fs;
                        LOG.debug("Trying to recover file lease {}", partPath);
                        dfs.recoverLease(partPath);
                        boolean isclosed = dfs.isFileClosed(partPath);
                        StopWatch sw = new StopWatch();
                        sw.start();
                        while (!isclosed) {
                            if (sw.getTime() > asyncTimeout) {
                                break;
                            }
                            try {
                                Thread.sleep(500);
                            } catch (InterruptedException e1) {
                                // ignore it
                            }
                            isclosed = dfs.isFileClosed(partPath);
                        }
                    }
                    Boolean truncated = (Boolean) refTruncate.invoke(fs, partPath, validLength);
                    if (!truncated) {
                        LOG.debug("Truncate did not immediately complete for {}, waiting...", partPath);

                        // we must wait for the asynchronous truncate operation to complete
                        StopWatch sw = new StopWatch();
                        sw.start();
                        long newLen = fs.getFileStatus(partPath).getLen();
                        while (newLen != validLength) {
                            if (sw.getTime() > asyncTimeout) {
                                break;
                            }
                            try {
                                Thread.sleep(500);
                            } catch (InterruptedException e1) {
                                // ignore it
                            }
                            newLen = fs.getFileStatus(partPath).getLen();
                        }
                        if (newLen != validLength) {
                            throw new RuntimeException("Truncate did not truncate to right length. Should be " + validLength + " is " + newLen + ".");
                        }
                    }
                } else {
                    Path validLengthFilePath = getValidLengthPathFor(partPath);
                    if (!fs.exists(validLengthFilePath) && fs.exists(partPath)) {
                        LOG.debug("Writing valid-length file for {} to specify valid length {}", partPath, validLength);
                        try (FSDataOutputStream lengthFileOut = fs.create(validLengthFilePath)) {
                            lengthFileOut.writeUTF(Long.toString(validLength));
                        }
                    }
                }

            } catch (IOException e) {
                LOG.error("Error while restoring BucketingSink state.", e);
                throw new RuntimeException("Error while restoring BucketingSink state.", e);
            } catch (InvocationTargetException | IllegalAccessException e) {
                LOG.error("Could not invoke truncate.", e);
                throw new RuntimeException("Could not invoke truncate.", e);
            }
        }
    }

    private void handlePendingFilesForPreviousCheckpoints(Map<Long, List<String>> pendingFilesPerCheckpoint) {
        // Move files that are confirmed by a checkpoint but did not get moved to final location
        // because the checkpoint notification did not happen before a failure

        LOG.debug("Moving pending files to final location on restore.");

        Set<Long> pastCheckpointIds = pendingFilesPerCheckpoint.keySet();
        for (Long pastCheckpointId : pastCheckpointIds) {
            // All the pending files are buckets that have been completed but are waiting to be renamed
            // to their final name
            for (String filename : pendingFilesPerCheckpoint.get(pastCheckpointId)) {
                Path finalPath = new Path(filename);
                Path pendingPath = getPendingPathFor(finalPath);

                try {
                    if (fs.exists(pendingPath)) {
                        LOG.debug("Restoring BucketingSink State: Moving pending file {} to final location after complete checkpoint {}.", pendingPath, pastCheckpointId);
                        fs.rename(pendingPath, finalPath);
                    }
                } catch (IOException e) {
                    LOG.error("Restoring BucketingSink State: Error while renaming pending file {} to final path {}: {}", pendingPath, finalPath, e);
                    throw new RuntimeException("Error while renaming pending file " + pendingPath + " to final path " + finalPath, e);
                }
            }
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        LOG.info("TIME TO subTask ={} notifyCheckpointComplete TIME ={}", getRuntimeContext().getIndexOfThisSubtask(), simpleDateFormat.format(new Date()));
        synchronized (state.bucketStates) {

            Iterator<Map.Entry<String, BucketState>> bucketStatesIt = state.bucketStates.entrySet().iterator();
            while (bucketStatesIt.hasNext()) {
                BucketState bucketState = bucketStatesIt.next().getValue();
                synchronized (bucketState.pendingFilesPerCheckpoint) {

                    Iterator<Map.Entry<Long, List<String>>> pendingCheckpointsIt =
                            bucketState.pendingFilesPerCheckpoint.entrySet().iterator();

                    while (pendingCheckpointsIt.hasNext()) {

                        Map.Entry<Long, List<String>> entry = pendingCheckpointsIt.next();
                        Long pastCheckpointId = entry.getKey();
                        List<String> pendingPaths = entry.getValue();

                        if (pastCheckpointId <= checkpointId) {
                            LOG.debug("Moving pending files to final location for checkpoint {}", pastCheckpointId);

                            for (String filename : pendingPaths) {
                                Path finalPath = new Path(filename);
                                Path pendingPath = getPendingPathFor(finalPath);

                                fs.rename(pendingPath, finalPath);
                                LOG.debug(
                                        "Moving pending file {} to final location having completed checkpoint {}.",
                                        pendingPath,
                                        pastCheckpointId);
                            }
                            pendingCheckpointsIt.remove();
                        }
                    }

                    if (!bucketState.isWriterOpen &&
                            bucketState.pendingFiles.isEmpty() &&
                            bucketState.pendingFilesPerCheckpoint.isEmpty()) {

                        // We've dealt with all the pending files and the writer for this bucket is not currently open.
                        // Therefore this bucket is currently inactive and we can remove it from our state.
                        bucketState.timerService.shutdownNow();
                        bucketStatesIt.remove();
                        LOG.info("TIME TO subTask ={} remove bucketstate TIME ={}", getRuntimeContext().getIndexOfThisSubtask(), simpleDateFormat.format(new Date()));

                    }
                }
            }
        }
    }

    /**
     * 桶信息维护
     *
     * @param
     */
    static final class BucketState implements Serializable {
        private static final long serialVersionUID = 1L;
        String bucketPath;
        String currentFile;
        long currentFileValidLength = -1L;
        long lastWrittenToTime;
        long creationTime;
        /**
         * 被pending的文件列表
         */
        List<String> pendingFiles = new ArrayList();
        final Map<Long, List<String>> pendingFilesPerCheckpoint = new HashMap();
        private transient int partCounter;
        private transient boolean isWriterOpen;
        private transient Writer<List<String>> writer;
        private transient int currentBatchId;
        protected transient ScheduledThreadPoolExecutor timerService;
        private transient BatchCollection currentBatchCollection;

        @Override
        public String toString() {
            return "In-progress=" + this.currentFile + " validLength=" + this.currentFileValidLength + " pendingForNextCheckpoint=" + this.pendingFiles + " pendingForPrevCheckpoints=" + this.pendingFilesPerCheckpoint + " lastModified@" + this.lastWrittenToTime;
        }

        BucketState(String bucketPath, BatchSinkConfig conf) {
//            this.lastWrittenToTime = lastWrittenToTime;
            this.timerService = new ScheduledThreadPoolExecutor(1, new ThreadFactoryBuilder().setNameFormat("batchsink-timerservice-" + currentBatchId + "-%d").build());
            this.currentBatchId = -1;
            this.bucketPath = bucketPath;
        }

        public void resetBatch(long currentProcessingTime, int batchSize, long batchLingerMs, AbstractBatchBucketSinkFunction batchFunction) {
            currentBatchCollection = new BatchCollection(++currentBatchId, batchSize);
            ScheduledFuture<?> timerFuture = timerService.schedule(() -> {
                if (currentBatchCollection.batchReady.compareAndSet(false, true)) {
                    synchronized (currentBatchCollection.lock) {
                        try {
                            processBatch(currentProcessingTime + batchLingerMs, batchFunction);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }, batchLingerMs, TimeUnit.MILLISECONDS);

            currentBatchCollection.setTimerFuture(timerFuture);
        }

        public void processBatch(long currentProcessingTime, AbstractBatchBucketSinkFunction batchFunction) throws Exception {
            //可能存在时间到了，但是文件关闭的场景
            LOG.info("TIME TO subTask ={} processBatch TIME ={}", batchFunction.getRuntimeContext().getIndexOfThisSubtask(), batchFunction.simpleDateFormat.format(new Date(currentProcessingTime)));
            if (!isWriterOpen) {
                batchFunction.openNewPartFile(new Path(this.bucketPath), this);
            }
            this.lastWrittenToTime = currentProcessingTime;
            if (0 == currentBatchCollection.getBatchCount()) {
                return;
            }
            Runnable r = new BatchProcessRunnable(writer, currentBatchCollection, batchFunction);
            if (null == batchFunction.batchProcessExecutor) {
                //process batch in disruptor consumer thread
                LOG.info("TIME TO subTask ={} initializeState TIME ={}", batchFunction.getRuntimeContext().getIndexOfThisSubtask(), batchFunction.simpleDateFormat.format(new Date()));

                r.run();
            } else {
                //process batch in exclusive executor
                LOG.info("TIME TO subTask ={} initializeState TIME ={}", batchFunction.getRuntimeContext().getIndexOfThisSubtask(), batchFunction.simpleDateFormat.format(new Date()));
                batchFunction.batchProcessExecutor.execute(r);
            }

        }

    }

    /**
     * 保存所有桶状态信息
     *
     * @param <T>
     */
    static final class State<T> implements Serializable {
        private static final long serialVersionUID = 1L;
        final Map<String, BucketState> bucketStates = new HashMap();

        State() {
        }

        void addBucketState(Path bucketPath, BucketState state) {
            synchronized (this.bucketStates) {
                this.bucketStates.put(bucketPath.toString(), state);
            }
        }

        BucketState getBucketState(Path bucketPath) {
            synchronized (this.bucketStates) {
                return (BucketState) this.bucketStates.get(bucketPath.toString());
            }
        }

        @Override
        public String toString() {
            return this.bucketStates.toString();
        }
    }

    private static class BatchCollection {

        @Getter
        private int batchSize;
        @Getter
        private int batchCount;
        @Getter
        private int batchId;
        @Getter
        @Setter
        private int retry;
        @Setter
        private ScheduledFuture<?> timerFuture;
        @Getter
        private List<String> elements;
        @Getter
        private long batchStartTime;
        @Getter
        @Setter
        private AtomicBoolean batchReady;

        private final Object lock;

        BatchCollection(int batchId, int batchSize) {
            this.batchId = batchId;
            this.batchSize = batchSize;
            this.batchCount = 0;
            this.elements = new ArrayList<>(batchSize);
            this.retry = 0;
            this.batchReady = new AtomicBoolean(false);
            this.lock = new Object();
        }

        void add(String element) {
            if (null == element) {
                return;
            }
            elements.add(element);
            ++batchCount;
            if (1 == batchCount) {
                batchStartTime = System.currentTimeMillis();
            }
        }

        void cancelLingerTimer() {
            if (null != timerFuture) {
                timerFuture.cancel(true);
                timerFuture = null;
            }
        }
        boolean isBatchFull() {
            return batchCount >= batchSize;
        }

        @Override
        public String toString() {
            return String.format("%s[batchId=%d, batchCount=%d, batchStartTime=%d, retry=%d]", getClass().getSimpleName(), batchId, batchCount, batchStartTime, retry);
        }
    }

    private static class BatchProcessRunnable implements Runnable, Comparable<BatchProcessRunnable> {
        private Writer writer;
        private BatchCollection collection;
        private IBatchProcessCallBack callback;
        private AbstractBatchBucketSinkFunction batchSinkFunction;

        BatchProcessRunnable(Writer writer, BatchCollection collection, AbstractBatchBucketSinkFunction batchSinkFunction) {
            this.writer = writer;
            this.collection = collection;
            this.batchSinkFunction = batchSinkFunction;
            this.callback = new BatchProcessCallBackImpl(collection, batchSinkFunction);
        }

        @Override
        public int compareTo(BatchProcessRunnable o) {
            long result = collection.getBatchStartTime() - o.collection.getBatchStartTime();
            return 0 == result ? (collection.getBatchId() - o.collection.getBatchId()) : (int) result;
        }

        @Override
        public void run() {
            try {
//                writer.write(collection.getElements().toString());
                writer.write(org.apache.commons.lang.StringUtils.join(collection.getElements(), "\n"));
                batchSinkFunction.process(collection.getElements(), callback);
            } catch (Exception e) {
                callback.completeExceptionally(e);
            }
        }
    }

    @AllArgsConstructor
    private static class BatchProcessCallBackImpl implements IBatchProcessCallBack {
        private static final Logger LOG = LoggerFactory.getLogger(BatchProcessCallBackImpl.class);
        private BatchCollection batchCollection;
        private AbstractBatchBucketSinkFunction batchSinkFunction;

        @Override
        public void complete() {
//            batchSinkFunction.pending.add(-batchCollection.getBatchCount());
        }

        @Override
        public void completeExceptionally(Exception error) {
            LOG.error("occur exception");
        }

    }
}
