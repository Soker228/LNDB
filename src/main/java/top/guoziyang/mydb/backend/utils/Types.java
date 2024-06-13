package top.guoziyang.mydb.backend.utils;

/**
 * 页号 + 偏移量
 */
public class Types {
    public static long addressToUid(int pgno, short offset) {
        long u0 = (long)pgno;
        long u1 = (long)offset;
        return u0 << 32 | u1; //或运算全 0 则 0, 见 1 则 1
    }
}