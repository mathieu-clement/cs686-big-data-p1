package edu.usfca.cs.dfs;

import org.junit.jupiter.api.Test;

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

        Set<Integer> random = Utils.chooseNrandom(n, set);
        assertEquals(n, random.size());

        for (Integer integer : random) {
            assertTrue(set.contains(integer));
        }
    }

}