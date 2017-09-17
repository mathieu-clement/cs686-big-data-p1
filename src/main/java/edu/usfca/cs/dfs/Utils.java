package edu.usfca.cs.dfs;

import java.util.*;

public class Utils {
    public static <E> Set<E> chooseNrandom(int n, Set<E> set) {
        List<E> list = new ArrayList<>(set);
        Collections.shuffle(list);
        return new HashSet<>(list.subList(0, n));
    }
}
