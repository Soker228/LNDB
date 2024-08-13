package top.guoziyang.mydb.backend.dm.page;

import java.util.Arrays;

import top.guoziyang.mydb.backend.dm.pageCache.PageCache;
import top.guoziyang.mydb.backend.utils.Parser;

/**
 * PageX 普通页管理器
 * 提供了一个由静态方法构成的工具类，用来对普通页管理。
 * 一个普通页面以一个 2 字节无符号数起始，表示这一页指针的偏移（即这一页写到了哪个位置）。
 * 剩下的部分都是实际存储的数据。所以对普通页的管理，基本都是围绕着对 FSO（Free Space Offset），也就是接下来往哪里插入数据进行的。
 *
 * 普通页结构
 * [FreeSpaceOffset] [Data]
 * FreeSpaceOffset: 2字节 空闲位置开始偏移
 * Data: 由于都是使用DataItem进行数据操作，所以Data的格式就是DataItem的格式：[ValidFlag] [DataSize] [Data]
 */
public class PageX {

    private static final short OF_FREE = 0;                                 // 数据页的起始位置
    private static final short OF_DATA = 2;                                 // 2字节
    public static final int MAX_FREE_SPACE = PageCache.PAGE_SIZE - OF_DATA; // 一页的最大剩余空间

    /**
     * 初始化一个空的 8k 页面，设置FSO为2字节
     * @return 空的数据页
     */
    public static byte[] initRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setFSO(raw, OF_DATA); // 初始化的第一个FSO就2字节
        return raw;
    }

    // 在写入之前获取 FSO，来确定写入的位置，并在写入之后更新 FSO
    /**
     * 设置数据页前2个字节中存的数据位置偏移量（FSO）
     * @param raw 数据
     * @param ofData 偏移量
     */
    private static void setFSO(byte[] raw, short ofData) {
        System.arraycopy(Parser.short2Byte(ofData), 0, raw, OF_FREE, OF_DATA);
    }

    /**
     * 获取数据页前 2 个字节中存的数据位置偏移量（FSO）
     * @param pg 数据页
     * @return
     */
    public static short getFSO(Page pg) {
        return getFSO(pg.getData());
    }
    private static short getFSO(byte[] raw) {
        return Parser.parseShort(Arrays.copyOfRange(raw, 0, 2));
    }

    /**
     * 向页面中插入数据
     * 将raw数据插入pg中，返回插入位置
     * @param pg 目标数据页
     * @param raw 插入的数据（DataItem格式的数据包）
     * @return 插入位置
     */
    public static short insert(Page pg, byte[] raw) {
        pg.setDirty(true);
        // 获取该数据页的空闲位置偏移量（FSO）
        short offset = getFSO(pg.getData());
        // 将raw的内容插入pg数据页的指定位置
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
        // 更新数据页前2个字节的FSO值
        setFSO(pg.getData(), (short)(offset + raw.length));
        return offset;
    }

    // 获取页面的空闲空间大小
    public static int getFreeSpace(Page pg) {
        return PageCache.PAGE_SIZE - (int)getFSO(pg.getData());
    }


    // 以下剩余两个函数 recoverInsert() 和 recoverUpdate() 用于在数据库崩溃后重新打开时（即第一页的校验不对时），恢复例程直接插入数据以及修改数据使用。
    /**
     * 直接插入数据页，将raw数据插入pg中的指定的offset位置，更新FSO(将pg的offset设置为较大的offset)
     * @param pg
     * @param raw
     * @param offset
     */
    public static void recoverInsert(Page pg, byte[] raw, short offset) {
        pg.setDirty(true);
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);

        short rawFSO = getFSO(pg.getData());
        if(rawFSO < offset + raw.length) {
            setFSO(pg.getData(), (short)(offset+raw.length));
        }
    }

    //TODO 为什么offset变了却不更新?
    /**
     * 修改数据页，将raw插入pg中的offset位置，不更新FSO
     * @param pg
     * @param raw
     * @param offset
     */
    public static void recoverUpdate(Page pg, byte[] raw, short offset) {
        pg.setDirty(true);
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
    }
}
