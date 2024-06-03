package top.guoziyang.mydb.backend.dm.dataItem;

import java.util.Arrays;

import com.google.common.primitives.Bytes;

import top.guoziyang.mydb.backend.common.SubArray;
import top.guoziyang.mydb.backend.dm.DataManagerImpl;
import top.guoziyang.mydb.backend.dm.page.Page;
import top.guoziyang.mydb.backend.utils.Parser;
import top.guoziyang.mydb.backend.utils.Types;

/**
 * DataItem 是 DM 层向上层提供的数据抽象。修改数据页全靠传递 DataItem 实现。
 * 上层模块通过地址，向 DM 请求到对应的 DataItem，再获取到其中的数据。
 * 在上层模块试图对 DataItem 进行修改时，需要遵循一定的流程：
 * 在修改之前需要调用 before() 方法，想要撤销修改时，调用 unBefore() 方法，在修改完成后，调用 after() 方法。
 * 整个流程，主要是为了保存前相数据，并及时落日志。DM 会保证对 DataItem 的修改是原子性的。
 */
public interface DataItem {
    SubArray data();        // 通过共享内存的方式返回数据

    void before();          // 修改数据前的方法，打开写锁

    void unBefore();        // 撤销修改，

    void after(long xid);   // 修改数据完成后的方法，记录此事务的修改操作到日志，关闭写锁

    void release();         // 释放 DataItem 缓存

    void lock();            // 打开写锁

    void unlock();          // 关闭写锁

    void rLock();           // 打开读锁

    void rUnLock();         // 关闭读锁

    Page page();            // 获取此 DataItem 所在的数据页

    long getUid();          // 获取 DataItem 的key

    byte[] getOldRaw();     // 获取修改时暂存的旧数据

    SubArray getRaw();      //

    // 打包为 dataItem 格式的数据包
    public static byte[] wrapDataItemRaw(byte[] raw) {
        byte[] valid = new byte[1];
        byte[] size = Parser.short2Byte((short) raw.length);
        return Bytes.concat(valid, size, raw);
    }

    // 从页面的offset处解析处DataItem
    public static DataItem parseDataItem(Page pg, short offset, DataManagerImpl dm) {
        byte[] raw = pg.getData();
        // 读取dataitem的大小
        short size = Parser.parseShort(Arrays.copyOfRange(raw, offset + DataItemImpl.OF_SIZE, offset + DataItemImpl.OF_DATA));
        short length = (short) (size + DataItemImpl.OF_DATA);
        // uid = 页号 + 偏移量
        long uid = Types.addressToUid(pg.getPageNumber(), offset);
        return new DataItemImpl(new SubArray(raw, offset, offset + length), new byte[length], pg, uid, dm);
    }

    public static void setDataItemRawInvalid(byte[] raw) {
        raw[DataItemImpl.OF_VALID] = (byte) 1;
    }
}
