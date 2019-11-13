package com.de314.data.local.utils;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.google.common.base.Functions;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.Enumeration;
import java.util.UUID;
import java.util.function.Function;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

@Slf4j
public class FileUtils {

    public static final String SEPARATOR = FileSystems.getDefault().getSeparator();

    public static long directorySize(String path) {
            Path folder = Paths.get(path);
        try {
            return Files.walk(folder)
                    .filter(p -> p.toFile().isFile())
                    .mapToLong(p -> p.toFile().length())
                    .sum();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return -1L;
    }

    public static String toPrettySize(long size) {
        String[] units = new String[] { "B", "KB", "MB", "GB", "TB" };
        int unitIndex = (int) Math.max(0, Math.log10(size) / 3);
        double unitValue = 1 << (unitIndex * 10);

        return new DecimalFormat("#,##0.#")
                .format(size / unitValue) + " "
                + units[unitIndex];
    }

    public static boolean delete(String path) {
        File file = new File(path);
        if (file.exists()) {
            if (file.isDirectory()) {
                return deleteFolder(file);
            } else {
                return delete(file);
            }
        }
        return true;
    }

    private static boolean deleteFolder(File folder) {
        File[] files = folder.listFiles();
        boolean result = true;
        if (files != null) { //some JVMs return null for empty dirs
            for (File f : files) {
                if (f.isDirectory()) {
                    result = result && deleteFolder(f);
                } else {
                    result = result && delete(f);
                }
            }
        }
        return result && delete(folder);
    }

    private static boolean delete(File f) {
        boolean result = f.delete();
        if (!result) {
            log.warn("Unable to delete {}", f.getPath());
        }
        return result;
    }

    // http://www.java2s.com/Code/Java/File-Input-Output/Makingazipfileofdirectoryincludingitssubdirectoriesrecursively.htm
    public static boolean zipDir(String sourcePath, String targetFilename) {
        try {
            ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(targetFilename));
            File source = new File(sourcePath);
            if (!source.exists() && source.isDirectory()) {
                return false;
            }
            zipDir(source, zos);
            zos.close();
            // TODO: encrypt???
            return true;
        } catch(Throwable t) {
            t.printStackTrace();
        }
        return false;
    }

    private static void zipDir(File dir, ZipOutputStream zos) throws IOException {
        if (!dir.exists() && dir.isDirectory()) {
            return;
        }
        String[] dirList = dir.list();
        byte[] readBuffer = new byte[2156];
        int bytesIn;
        for (int i = 0; i < dirList.length; i++) {
            File f = new File(dir, dirList[i]);
            if (f.isDirectory()) {
                zipDir(f, zos);
            } else {
                FileInputStream fis = new FileInputStream(f);
                ZipEntry anEntry = new ZipEntry(f.getPath());
                zos.putNextEntry(anEntry);
                while ((bytesIn = fis.read(readBuffer)) != -1) {
                    zos.write(readBuffer, 0, bytesIn);
                }
                //close the Stream
                fis.close();
            }
        }
    }

    public static boolean unzipDir(String archiveFilename, Function<String, String> pathMapper) {
        try {
            ZipFile zipFile = new ZipFile(archiveFilename);

            Enumeration enumeration = zipFile.entries();
            if (pathMapper == null) {
                pathMapper = Functions.identity();
            }
            boolean upsertParents = true;
            while (enumeration.hasMoreElements()) {
                ZipEntry zipEntry = (ZipEntry) enumeration.nextElement();
                String srcName = zipEntry.getName();
                String targetName = pathMapper.apply(srcName);
                log.debug("Unzipping: {} => {}", srcName, targetName);
                if (upsertParents) {
                    new File(targetName).getParentFile().mkdirs();
                    upsertParents = false;
                }

                int size;
                byte[] buffer = new byte[2048];
                BufferedInputStream bis = new BufferedInputStream(zipFile.getInputStream(zipEntry));
                BufferedOutputStream bos = new BufferedOutputStream(
                        new FileOutputStream(targetName),
                        buffer.length
                );
                while ((size = bis.read(buffer, 0, buffer.length)) != -1) {
                    bos.write(buffer, 0, size);
                }
                bos.flush();
                bos.close();
                bis.close();
            }
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    public static boolean upload(SpaceConfig config, String srcFilename, String destFilename) {
        log.info("Uploading {}", srcFilename);
        try {
            AmazonS3 space = getSpace(config);

            File srcFile = new File(srcFilename);
            String remoteFilename = put(space, config, srcFile, destFilename);
            log.info("Uploaded remote file {}", remoteFilename);
            return remoteFilename != null;
        } catch (Throwable t) {
            t.printStackTrace();
        }
        return false;
    }

    private static AmazonS3 getSpace(SpaceConfig config) {
        AWSCredentialsProvider awscp = new AWSStaticCredentialsProvider(
                new BasicAWSCredentials(config.getAccessToken(), config.getSecret())
        );
        return AmazonS3ClientBuilder
                .standard()
                .withCredentials(awscp)
                .withEndpointConfiguration(
                        new AwsClientBuilder.EndpointConfiguration(config.getServiceEndpoint(), config.getRegion())
                )
                .build();
    }

    private static String put(AmazonS3 space, SpaceConfig config, File srcFile, String putFilepath) throws FileNotFoundException {
        if (!srcFile.exists()) {
            throw new FileNotFoundException(srcFile.getAbsolutePath());
        }
        InputStream is = new FileInputStream(srcFile);
        ObjectMetadata om = new ObjectMetadata();
        om.setContentLength(srcFile.length());
        space.putObject(config.getBucketName(), putFilepath, is, om);
        return space.getUrl(config.getBucketName(), putFilepath).toString();
    }

    public static String getFromSpace(SpaceConfig config, String getFilepath) {
        File tmpDir = new File("data/tmp");
        if (!tmpDir.exists()) {
            tmpDir.mkdirs();
        }
        String reloadFilename = new StringBuilder("data")
                .append(SEPARATOR)
                .append("tmp")
                .append(SEPARATOR)
                .append(UUID.randomUUID().toString())
                .append(".zip")
                .toString();
        File reloadFile = new File(reloadFilename);

        AmazonS3 space = getSpace(config);

        ObjectMetadata om = space.getObject(
                new GetObjectRequest(config.getBucketName(), getFilepath),
                reloadFile
        );

        log.info("reloading {}", om);

        return reloadFilename;
    }

    @Value
    @Builder
    public static class SpaceConfig {
        @NonNull private String accessToken;
        @NonNull private String secret;
        @NonNull
        @Builder.Default
        private String bucketName = "api-camp-data-backup";
        @NonNull
        @Builder.Default
        private String serviceEndpoint = "nyc3.digitaloceanspaces.com";
        @NonNull
        @Builder.Default
        private String region = "nyc3";
    }
}
