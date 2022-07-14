package com.jd.easy.audience.task.plugin.util.jfs;

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import com.jd.jss.Credential;
import com.jd.jss.JingdongStorageService;
import com.jd.jss.client.ClientConfig;
import com.jd.jss.domain.Bucket;
import com.jd.jss.domain.ObjectListing;
import com.jd.jss.domain.ObjectSummary;
import com.jd.jss.domain.StorageObject;
import com.jd.jss.exception.StorageClientException;
import com.jd.jss.service.BucketService;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.attribute.FileAttribute;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.jfs.JFSCredentials;
import org.apache.hadoop.fs.jfs.JFSUtils;
import org.apache.hadoop.fs.jfs.SpaceObject;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

@Public
@Stable
public class JFSFileSystem extends FileSystem {
    public static final Log LOG = LogFactory.getLog(JFSFileSystem.class);

    private URI uri;

    private Path workingDir;

    private JingdongStorageService jss;

    private BucketService bucketService;

    private long blockSize;

    private long maxSkipSize;

    private File stagingDirectory;

    private String jfsEndPoint = "storage.jd.com";

    private String host = null;

    private static final String EQUAL_SIGN = "=";

    private static final String EQUAL_SIGN_JFS = "--";

    private static String dateFormat = "EEE, dd MMM yyyy HH:mm:ss z";

    private Path toJfsPath(Path path) {
        if (!path.toString().startsWith("jfs://")) {
            return new Path("jfs://" + this.jfsEndPoint + "/" + path.toString().replaceAll("=", "--"));
        }
        return new Path(path.toString().replaceAll("=", "--"));
    }

    @Override
    public synchronized void initialize(URI uri, Configuration conf) throws IOException {
        LOG.debug("initialize, uri:" + uri);
        super.initialize(uri, conf);
        JFSCredentials jfsCredentials = new JFSCredentials();
        jfsCredentials.initialize(uri, conf);
        Credential credential = new Credential(jfsCredentials.getAccessKey(), jfsCredentials.getSecretAccessKey());
        this.jfsEndPoint = jfsCredentials.getEndPoint();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setSocketTimeout(100000);
        clientConfig.setConnectionTimeout(50000);
        if (StringUtils.isNotBlank(this.jfsEndPoint)) {
            clientConfig.setEndpoint(this.jfsEndPoint);
        }
        clientConfig.setPartSize(1073741824L * 2);
        clientConfig.setMaxConnections(128);
        this.jss = new JingdongStorageService(credential, clientConfig);
        if (StringUtils.endsWith(uri.toString(), ":/") || StringUtils.endsWith(uri.toString(), ":///")) {
            this.bucketService = null;
            conf.set("fs.default.name", "0.0.0.0:9000");
        } else {
            this.bucketService = this.jss.bucket(uri.getHost());
            this.host = uri.getHost();
            this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
            this.workingDir = (new Path("/")).makeQualified(this.uri, new Path("/"));
        }
        this.blockSize = conf.getLong("fs.jfs.block.size", 67108864L);
        this.maxSkipSize = conf.getLong("fs.jfs.skip.size", 1048576L);
        this.stagingDirectory = new File(System.getProperty("java.io.tmpdir"));
        setConf(conf);
    }

    @Override
    public String getScheme() {
        return "jfs";
    }

    @Override
    public URI getUri() {
        return this.uri;
    }

    @Override
    public Path getWorkingDirectory() {
        return this.workingDir;
    }

    @Override
    public void setWorkingDirectory(Path dir) {
        this.workingDir = makeAbsolute(dir);
    }

    private Path makeAbsolute(Path path) {
        if (path.isAbsolute()) {
            return path;
        }
        return new Path(this.workingDir, path);
    }

    private Path qualifiedPath(Path path) {
        Path qualified = path.makeQualified(this.uri, getWorkingDirectory());
        return qualified;
    }

    @Override
    public synchronized boolean mkdirs(Path path, FsPermission permission) {
        LOG.debug("mkdirs: " + path.toString());
        String pathToKey = JFSUtils.pathToKey(path);
        if (!pathToKey.endsWith("/")) {
            pathToKey = pathToKey + "/";
        }
        String put = this.bucketService.object(pathToKey).put();
        if (Strings.isNullOrEmpty(put)) {
            LOG.error("Failed to create directory " + path);
            return false;
        }
        return true;
    }

    @Override
    public boolean isFile(Path path) throws IOException {
        return getFileStatus(path).isFile();
    }

    private boolean directory(Path path) throws IOException {
        return getFileStatus(path).isDirectory();
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        LOG.debug("listStatus, path:" + path);
        Path absolutePath = makeAbsolute(path);
        List<LocatedFileStatus> list = new ArrayList<>();
        RemoteIterator<LocatedFileStatus> iterator = listLocatedStatus(absolutePath);
        while (iterator.hasNext()) {
            list.add(iterator.next());
        }
        return (FileStatus[]) Iterators.toArray(list.iterator(), LocatedFileStatus.class);
    }

    @Override
    public RemoteIterator<LocatedFileStatus> listLocatedStatus(final Path path) {
        LOG.debug("listLocatedStatus, path:" + path);
        return new RemoteIterator<LocatedFileStatus>() {
            private final Iterator<LocatedFileStatus> iterator = JFSFileSystem.this.listPrefix(path);

            @Override
            public boolean hasNext() {
                return this.iterator.hasNext();
            }

            @Override
            public LocatedFileStatus next() {
                return this.iterator.next();
            }
        };
    }

    private synchronized Iterator<LocatedFileStatus> listPrefix(Path path) {
        LOG.debug("listPrefix, path:" + path);
        String key = JFSUtils.pathToKey(path);
        if (!key.isEmpty()) {
            key = key + "/";
        }
        if (StringUtils.isNotBlank(this.host)) {
            this.bucketService.prefix(key);
            this.bucketService.delimiter("/");
            ObjectListing list1 = this.bucketService.listObject();
            List<SpaceObject> list2 = Collections.synchronizedList(new ArrayList());
            List commonPrefixes1 = list1.getCommonPrefixes();
            if (commonPrefixes1 != null && commonPrefixes1.size() > 0) {
                Iterator<String> bucket1 = commonPrefixes1.iterator();
                while (bucket1.hasNext()) {
                    String creationDate = bucket1.next();
                    SpaceObject location1 = new SpaceObject();
                    ObjectSummary bucketName1 = new ObjectSummary();
                    bucketName1.setKey(creationDate);
                    bucketName1.setSize(0L);
                    location1.setDirectory(true);
                    location1.setObjectSummary(bucketName1);
                    list2.add(location1);
                }
            }
            List bucket2 = list1.getObjectSummaries();
            if (bucket2 != null && bucket2.size() > 0) {
                Iterator<ObjectSummary> creationDate1 = bucket2.iterator();
                while (creationDate1.hasNext()) {
                    ObjectSummary location2 = creationDate1.next();
                    SpaceObject bucketName2 = new SpaceObject();
                    bucketName2.setObjectSummary(location2);
                    list2.add(bucketName2);
                }
            }
            return statusFromObjects((List) list2);
        }
        List list = this.jss.listBucket();
        List<LocatedFileStatus> fileStatusList = Collections.synchronizedList(new ArrayList());
        Iterator<Bucket> commonPrefixes = list.iterator();
        while (commonPrefixes.hasNext()) {
            Bucket bucket = commonPrefixes.next();
            String creationDate = bucket.getCreationDate();
            String location = bucket.getLocation();
            String bucketName = bucket.getName();
            FileStatus fileStatus = new FileStatus(0L, true, 1, 0L, parseDateTime(creationDate).getTime(), new Path("jfs://" + bucketName));
            fileStatusList.add(createLocatedFileStatus(fileStatus));
        }
        return (Iterator) fileStatusList.iterator();
    }

    private synchronized Iterator<LocatedFileStatus> statusFromObjects(List<SpaceObject> list) {
        LOG.debug("statusFromObjects, list:" + list);
        List<LocatedFileStatus> fileStatusList = Collections.synchronizedList(new ArrayList<>());
        for (SpaceObject object : list) {
            FileStatus fileStatus = null;
            if (object.isDirectory()) {
                fileStatus = new FileStatus(0L, true, 1, this.blockSize, 0L, qualifiedPath(new Path(getWorkingDirectory() + object.getObjectSummary().getKey())));
            } else {
                fileStatus = new FileStatus(object.getObjectSummary().getSize(), false, 1, this.blockSize, parseDateTime(object.getObjectSummary().getLastModified()).getTime(), qualifiedPath(new Path(getWorkingDirectory() + object.getObjectSummary().getKey())));
            }
            fileStatusList.add(createLocatedFileStatus(fileStatus));
        }
        return fileStatusList.iterator();
    }

    private LocatedFileStatus createLocatedFileStatus(FileStatus status) {
        LOG.debug("createLocatedFileStatus: " + status);
        try {
            BlockLocation[] fakeLocation = getFileBlockLocations(status, 0L, status.getLen());
            return new LocatedFileStatus(status, fakeLocation);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private static Date parseDateTime(String date) {
        try {
            return (new SimpleDateFormat(dateFormat, Locale.US)).parse(date);
        } catch (ParseException e) {
            LOG.error("Failed to parse dateTime=" + date, e);
            return null;
        }
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) {
        throw new UnsupportedOperationException("Append Not supported");
    }

    @Override
    public synchronized FSDataOutputStream create(Path path, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
        LOG.debug("Create " + path.toString());
        if (!overwrite && exists(toJfsPath(path))) {
            throw new IOException("File already exists:" + path);
        }
        if (!this.stagingDirectory.exists()) {
            Files.createDirectories(this.stagingDirectory.toPath(), (FileAttribute<?>[]) new FileAttribute[0]);
        }
        File tempFile = Files.createTempFile(this.stagingDirectory.toPath(), "jfs-", ".tmp", (FileAttribute<?>[]) new FileAttribute[0]).toFile();
        String key = JFSUtils.pathToKey(qualifiedPath(path));
        return new FSDataOutputStream(new JFSOutputStream(this.bucketService, key, tempFile), this.statistics);
    }

    @Override
    public void close() throws IOException {
        super.close();
        this.jss.destroy();
    }

    @Override
    public synchronized FSDataInputStream open(Path path, int bufferSize) throws IOException {
        return new FSDataInputStream((InputStream) new BufferedFSInputStream(new JFSInputStream(this.bucketService, toJfsPath(path), this.maxSkipSize), bufferSize));
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        boolean isDirectory;
        LOG.debug("delete path:" + path);
        path = toJfsPath(path);
        try {
            isDirectory = directory(path);
        } catch (FileNotFoundException e) {
            if (!path.toString().endsWith("/")) {
                LOG.debug("File " + path + " does not exist, try to delete it as an empty directory ");
                return deleteObject(JFSUtils.pathToKey(path) + "/");
            }
            LOG.warn("File " + path + " does not exist");
            return true;
        }
        if (!isDirectory) {
            return deleteObject(JFSUtils.pathToKey(path));
        }
        if (!recursive) {
            throw new IOException("Directory " + path + " is not empty");
        }
        boolean result = true;
        for (FileStatus file : listStatus(path)) {
            if (!delete(file.getPath(), true) && result) {
                result = false;
            }
        }
        return (deleteObject(JFSUtils.pathToKey(path)) && result);
    }

    private boolean deleteObject(String key) {
        LOG.debug("deleteObject key:" + key);
        try {
            this.jss.deleteObject(this.uri.getHost(), key);
        } catch (StorageClientException ex) {
            if ("NoSuchKey".equals(ex.getError().getCode())) {
                if (!key.endsWith("/")) {
                    LOG.debug("File " + key + " does not exist, try to delete it as an empty directory");
                    return deleteObject(key + "/");
                }
                LOG.warn("File " + key + " does not exist");
                return true;
            }
            LOG.error("File " + key + " failed to be deleted", (Throwable) ex);
            return false;
        } catch (IllegalArgumentException ex) {
            LOG.error("Failed to delete " + key, ex);
            return false;
        }
        return true;
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        LOG.debug("getFileStatus path:" + path);
        JfsMetadata jfsObjectMetadata = getJfsObjectMetadata(path);
        if (path.getName().isEmpty()) {
            try {
                if (jfsObjectMetadata != null) {
                    return new FileStatus(jfsObjectMetadata.getContentLength(), false, 1, this.blockSize, lastModifiedTime(jfsObjectMetadata), qualifiedPath(path));
                }
            } catch (Exception var5) {
                throw new FileNotFoundException("File does not exist: " + path);
            }
        }
        if (jfsObjectMetadata == null) {
            Iterator<LocatedFileStatus> iterator = listPrefix(path);
            if (iterator.hasNext()) {
                LocatedFileStatus next = iterator.next();
                return new FileStatus(0L, true, 1, 0L, 0L, qualifiedPath(path));
            }
            throw new FileNotFoundException("File does not exist: " + path);
        }
        return new FileStatus(jfsObjectMetadata.getContentLength(), false, 1, this.blockSize, lastModifiedTime(jfsObjectMetadata), qualifiedPath(path));
    }

    private JfsMetadata getJfsObjectMetadata(Path path) {
        StorageObject object;
        LOG.debug("getJfsObjectMetadata path:" + path);
        try {
            String metadata = JFSUtils.pathToKey(path);
            if (Strings.isNullOrEmpty(metadata)) {
                return null;
            }
            object = this.bucketService.object(metadata).head();
            if (object == null) {
                return null;
            }
        } catch (Exception var4) {
            return null;
        }
        JfsMetadata metadata1 = JfsMetadata.fromStorageObject(object);
        return metadata1;
    }

    private static long lastModifiedTime(JfsMetadata metadata) {
        Date date = metadata.getLastModified();
        return (date != null) ? date.getTime() : 0L;
    }

    private static class JfsMetadata {
        private long contentLength;

        private Date lastModified;

        private JfsMetadata(long contentLength, Date lastModified) {
            this.contentLength = contentLength;
            this.lastModified = lastModified;
        }

        public long getContentLength() {
            return this.contentLength;
        }

        public Date getLastModified() {
            return this.lastModified;
        }

        public static JfsMetadata fromStorageObject(StorageObject storageObject) {
            return new JfsMetadata(storageObject.getContentLength(), JFSFileSystem.parseDateTime(storageObject.getLastModified()));
        }
    }

    @Override
    public String getCanonicalServiceName() {
        return null;
    }

    @Override
    public synchronized boolean rename(Path srcPath, Path dstPath) throws IOException {
        LOG.debug("rename srcPath:" + srcPath + " dstPath:" + dstPath);
        if (srcPath.isRoot()) {
            LOG.error(srcPath + " cannot be renamed to " + dstPath + " because of the root of a filesystem");
            return false;
        }
        Path parent = dstPath.getParent();
        while (parent != null && !srcPath.equals(parent)) {
            parent = parent.getParent();
        }
        if (parent != null) {
            LOG.error("dstPath parent " + parent + " is not null," + srcPath + " cannot be renamed to " + dstPath);
            return false;
        }
        FileStatus srcStatus = getFileStatus(srcPath);
        FileStatus dstStatus = null;
        try {
            dstStatus = getFileStatus(dstPath);
        } catch (FileNotFoundException e) {
            LOG.info(dstPath + " does not exist, ignore it.");
        }
        if (dstStatus == null) {
            FileStatus dstStatus2 = getFileStatus(dstPath.getParent());
            if (!dstStatus2.isDirectory()) {
                LOG.error(dstPath + " does not exist and parent is not a directory");
                throw new IOException(String.format("Failed to rename %s to %s, %s is a file", new Object[]{srcPath, dstPath, dstPath.getParent()}));
            }
        } else {
            if (srcStatus.getPath().equals(dstStatus.getPath())) {
                LOG.error(srcStatus.getPath() + " is same as " + dstStatus.getPath() + " and srcPath is directory: " + srcStatus.isDirectory());
                return !srcStatus.isDirectory();
            }
            if (dstStatus.isDirectory()) {
                dstPath = new Path(dstPath, srcPath.getName());
                FileStatus[] statuses = null;
                try {
                    statuses = listStatus(dstPath);
                } catch (FileNotFoundException e) {
                    LOG.debug("listStatus(" + dstPath + ") got FileNotFoundException.", e);
                }
                if (statuses != null && statuses.length > 0) {
                    throw new FileAlreadyExistsException(String.format("Failed to rename %s to %s, file already exists or not empty!", new Object[]{srcPath, dstPath}));
                }
            } else {
                throw new FileAlreadyExistsException(String.format("Failed to rename %s to %s, file already exists!", new Object[]{srcPath, dstPath}));
            }
        }
        if (srcStatus.isDirectory()) {
            if (dstStatus != null) {
                if (dstStatus.isDirectory()) {
                    copyDirectory(srcPath, dstPath);
                } else {
                    LOG.error(" cannot rename, because " + srcPath + " is a directory, but dstPath is a file: " + dstPath);
                    return false;
                }
            } else if (mkdirs(dstPath)) {
                copyDirectory(srcPath, dstPath);
            } else {
                LOG.error(" cannot create directory " + dstPath);
                return false;
            }
        } else {
            copyFile(srcPath, new Path(dstPath, srcPath.getName()));
        }
        boolean same = srcPath.equals(dstPath);
        boolean deleted = false;
        try {
            deleted = delete(srcPath, true);
        } catch (Exception e) {
            LOG.warn("Failed to delete " + srcPath);
        }
        LOG.debug(srcPath + " is same as dstPath ? " + same + " and srcPath is deleted ? " + deleted);
        return (same || deleted);
    }

    private synchronized boolean copyFile(Path srcPath, Path dstPath) throws IOException {
        LOG.debug("copyFile srcPath:" + srcPath + " dstPath:" + dstPath);
        this.jss.bucket(this.host).object(JFSUtils.pathToKey(dstPath)).copyFrom(srcPath.toUri().getHost(), JFSUtils.pathToKey(srcPath)).copy();
        return delete(srcPath, true);
    }

    private boolean copyDirectory(Path srcPath, Path dstPath) throws IOException {
        LOG.debug("copyDirectory srcPath:" + srcPath + " dstPath:" + dstPath);
        if (dstPath.toString().startsWith(srcPath.toString())) {
            LOG.error("Cannot rename a directory to a subdirectory of self");
            return false;
        }
        FileStatus[] fileStatuses = listStatus(srcPath);
        for (FileStatus fileStatus : fileStatuses) {
            Path newKey = new Path(dstPath, fileStatus.getPath().toString().substring(srcPath.toString().length() + 1));
            if (fileStatus.isDirectory()) {
                copyDirectory(fileStatus.getPath(), newKey);
            } else {
                copyFile(fileStatus.getPath(), newKey);
            }
        }
        return true;
    }
}
