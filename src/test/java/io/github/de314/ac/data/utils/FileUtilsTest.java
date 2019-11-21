package io.github.de314.ac.data.utils;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class FileUtilsTest {

    @Test
    void directorySize() throws IOException {
        File dir = new File("data/test");
        dir.mkdirs();

        long emptySize = FileUtils.directorySize("data/test");
        String prettySize = FileUtils.toPrettySize(emptySize);
        System.out.println("prettySize = " + prettySize);
        FileOutputStream fos = new FileOutputStream("data/test/test.txt");
        fos.write("Hello, World!".getBytes(Charset.defaultCharset()));
        fos.flush();
        fos.close();

        long filledSize = FileUtils.directorySize("data/test");
        prettySize = FileUtils.toPrettySize(filledSize);
        System.out.println("prettySize = " + prettySize);

        assertNotEquals(emptySize, filledSize);

        FileUtils.delete("data/test");

        assertFalse(dir.exists());
    }

    @Test
    void zipzip() throws IOException {
        String srcPath = "data/zipTest";
        String destPath = "data/zipzap";
        String archivePath = String.format("data/%s.zip", UUID.randomUUID());
        File dir = new File(srcPath);
        dir.mkdirs();

        FileOutputStream fos = new FileOutputStream("data/zipTest/test.txt");
        fos.write("Hello, World!".getBytes(Charset.defaultCharset()));
        fos.flush();
        fos.close();

        boolean success = FileUtils.zipDir(srcPath, archivePath);
        assertTrue(success);
        assertTrue(new File(archivePath).exists());

        success = FileUtils.unzipDir(archivePath, filePath -> filePath.replaceAll(srcPath, destPath));
        assertTrue(success);
        assertTrue(new File(destPath).exists());
        assertTrue(new File("data/zipzap/test.txt").exists());

        FileUtils.delete(archivePath);
        FileUtils.delete(srcPath);
        FileUtils.delete(destPath);

    }
}