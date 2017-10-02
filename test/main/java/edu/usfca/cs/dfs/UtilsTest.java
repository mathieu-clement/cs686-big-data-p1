package edu.usfca.cs.dfs;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UtilsTest {
    @Test
    void chooseNrandom_NlessThanArraySize() {
        int n = 3;
        Set<Integer> set = new HashSet<>(Arrays.asList(1, 2, 3, 4, 5));

        Set<Integer> random = Utils.chooseNrandomOrMin(n, set);
        assertEquals(n, random.size());

        for (Integer integer : random) {
            assertTrue(set.contains(integer));
        }
    }

    @Test
    void testMd5sum() throws IOException {
        String content = "I like ice cream.\n";
        String expectedSum = "bbc3b8f636bbcf0b994f0698d25ca85c";

        File file = File.createTempFile("md5sumtest", "icecream");
        Utils.writeStringToFile(file.getAbsolutePath(), content);
        String actualSum = Utils.md5sum(file);
        file.delete();

        assertEquals(expectedSum, actualSum);

    }
}