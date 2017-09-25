package edu.usfca.cs.dfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class Utils {
    public static <E> Set<E> chooseNrandom(int n, Set<E> set) {
        List<E> list = new ArrayList<>(set);
        Collections.shuffle(list);
        return new HashSet<>(list.subList(0, n));
    }

    public static String md5sum(Path path) throws IOException {
        return md5sum(path.toFile());
    }

    public static String md5sum(File file) throws IOException {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] buf = new byte[1024];
            int c;

            FileInputStream fis = new FileInputStream(file);

            while ((c = fis.read(buf)) != -1) {
                md.update(buf, 0, c);
            }

            byte[] digest = md.digest();
            fis.close();
            return toHexString(digest);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            System.err.println("MD5 message digest not found, exiting.");
            System.exit(1);
            return null;
        }
    }

    private static String toHexString(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            // lowercase x for lowercase letters (a b c d e f)
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}
