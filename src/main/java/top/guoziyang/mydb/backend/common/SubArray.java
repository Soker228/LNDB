package top.guoziyang.mydb.backend.common;

/**
 * 模拟 python 和 go 的切片操作，实现一个共享内存的子数组，
 * 在 Java 中，当你执行类似 subArray 的操作时，只会在底层进行一个复制，无法同一片内存。
 * 因此，Java无法实现共享内存数组，这里单纯松散的规定数组的可使用范围，实现“共享”
 * “共享” 是指多个对象引用同一个数组对象，并不是多个对象共享同一片内存
 */
public class SubArray {
    public byte[] raw;
    public int start;
    public int end;

    public SubArray(byte[] raw, int start, int end) {
        this.raw = raw;
        this.start = start;
        this.end = end;
    }
}
