package com.de314.data.local.utils;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.google.common.collect.Lists;
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
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

@Slf4j
public class FileUtils {

    public static final String SEPARATOR = FileSystems.getDefault().getSeparator();
    public static final String SPACE_BUCKET_NAME = "api-camp-data-backup";
    public static final String SPACE_SERVICE_ENDPOINT = "nyc3.digitaloceanspaces.com";
    public static final String SPACE_REGION = "nyc3";

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
        int unitIndex = (int) (Math.log10(size) / 3);
        double unitValue = 1 << (unitIndex * 10);

        return new DecimalFormat("#,##0.#")
                .format(size / unitValue) + " "
                + units[unitIndex];
    }

    public static boolean deleteFolder(String path) {
        File folder = new File(path);
        if (folder.exists() && folder.isDirectory()) {
            deleteFolder(folder);
            return true;
        }
        return false;
    }

    public static void deleteFolder(File folder) {
        File[] files = folder.listFiles();
        if (files != null) { //some JVMs return null for empty dirs
            for (File f : files) {
                if (f.isDirectory()) {
                    deleteFolder(f);
                } else {
                    f.delete();
                }
            }
        }
        folder.delete();
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

    public static void unzipDir(String archiveFilename) throws Exception {
        ZipFile zipFile = new ZipFile(archiveFilename);
        Enumeration enumeration = zipFile.entries();
        while (enumeration.hasMoreElements()) {
            ZipEntry zipEntry = (ZipEntry) enumeration.nextElement();
            log.debug("Unzipping: {}", zipEntry.getName());
            BufferedInputStream bis = new BufferedInputStream(zipFile.getInputStream(zipEntry));
            int size;
            byte[] buffer = new byte[2048];
            BufferedOutputStream bos = new BufferedOutputStream(
                    new FileOutputStream(zipEntry.getName()), buffer.length);
            while ((size = bis.read(buffer, 0, buffer.length)) != -1) {
                bos.write(buffer, 0, size);
            }
            bos.flush();
            bos.close();
            bis.close();
        }
    }

    public static Function<String, List<String>> uploadArchiveStrategy(String accessKey, String secret, String namespace) {
        return filename -> upload(accessKey, secret, filename, namespace);
    }

    public static List<String> upload(String accessKey, String secret, String filename, String namespace) {
        log.info("Uploading namespace='{}': {}", namespace, filename);
        try {
            AWSCredentialsProvider awscp = new AWSStaticCredentialsProvider(
                    new BasicAWSCredentials(accessKey, secret)
            );
            AmazonS3 space = AmazonS3ClientBuilder
                    .standard()
                    .withCredentials(awscp)
                    .withEndpointConfiguration(
                            new AwsClientBuilder.EndpointConfiguration(SPACE_SERVICE_ENDPOINT, SPACE_REGION)
                    )
                    .build();

            List<String> urls = Lists.newArrayList(
                    put(space, filename, String.format("bak.v1/%s", new File(filename).getName()))
            );
            if (namespace != null) {
                urls.add(
                        put(space, filename, String.format("bak.v1/%s-latest.zip", namespace))
                );
            }
            return urls;
        } catch (Throwable t) {
            t.printStackTrace();
        }
        return null;
    }

    private static String put(AmazonS3 space, String filename, String putFilepath) throws FileNotFoundException {
        File file = new File(filename);
        InputStream is = new FileInputStream(file);
        ObjectMetadata om = new ObjectMetadata();
        om.setContentLength(file.length());
        space.putObject(SPACE_BUCKET_NAME, putFilepath, is, om);
        return space.getUrl(SPACE_BUCKET_NAME, putFilepath).toString();
    }

    public static boolean reload(String accessKey, String secret, String namespace) {
        log.info("Reloading namespace='{}'", namespace);
        try {
            AWSCredentialsProvider awscp = new AWSStaticCredentialsProvider(
                    new BasicAWSCredentials(accessKey, secret)
            );
            AmazonS3 space = AmazonS3ClientBuilder
                    .standard()
                    .withCredentials(awscp)
                    .withEndpointConfiguration(
                            new AwsClientBuilder.EndpointConfiguration(SPACE_SERVICE_ENDPOINT, SPACE_REGION)
                    )
                    .build();

            String getFilepath = String.format("bak.v1/%s-latest.zip", namespace);
            String tmpFile = get(space, getFilepath);
            unzipDir(tmpFile);

            return new File(tmpFile).delete();
        } catch (Throwable t) {
            t.printStackTrace();
        }
        return false;
    }



    private static String get(AmazonS3 space, String getFilepath) throws FileNotFoundException {
        File tmpDir = new File("tmp");
        if (!tmpDir.exists()) {
            tmpDir.mkdirs();
        }
        String reloadFilename = String.format("tmp%s%s.zip", SEPARATOR, UUID.randomUUID());
        File reloadFile = new File(reloadFilename);

        ObjectMetadata om = space.getObject(
                new GetObjectRequest(SPACE_BUCKET_NAME, getFilepath),
                reloadFile
        );

        log.info("reloading {}", om);

        return reloadFilename;
    }
}
