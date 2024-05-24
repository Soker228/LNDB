package top.guoziyang.mydb.backend.dm;

import top.guoziyang.mydb.backend.common.AbstractCache;
import top.guoziyang.mydb.backend.dm.dataItem.DataItem;
import top.guoziyang.mydb.backend.dm.dataItem.DataItemImpl;
import top.guoziyang.mydb.backend.dm.logger.Logger;
import top.guoziyang.mydb.backend.dm.page.Page;
import top.guoziyang.mydb.backend.dm.page.PageOne;
import top.guoziyang.mydb.backend.dm.page.PageX;
import top.guoziyang.mydb.backend.dm.pageCache.PageCache;
import top.guoziyang.mydb.backend.dm.pageIndex.PageIndex;
import top.guoziyang.mydb.backend.dm.pageIndex.PageInfo;
import top.guoziyang.mydb.backend.tm.TransactionManager;
import top.guoziyang.mydb.backend.utils.Panic;
import top.guoziyang.mydb.backend.utils.Types;
import top.guoziyang.mydb.common.Error;

/**
 * DataManager 是 DM 层直接对外提供方法的类，使用 DataItem 进行数据交互，同时也实现了 DataItem 对象的缓存，靠 UID 查询 DataItem 数据项。
 * 使用分页进行数据的处理，每个页面里有很多个 DataItem 数据项，也就是先找到数据页，再找到 DataItem 数据项进行读写；
 * uid 是由页号和页内偏移组成的一个 8 字节无符号整数，页号和偏移各占 4 字节，所以通过 uid 就可以快速定位 DataItem 数据的位置；
 *      DM向上层提供了三个功能：读、插入和修改。
 *      修改是通过读出的 DataItem 然后再插入回去实现的，所以 DataManager 只需要提供 read() 和 insert() 方法操作 DataItem 即可
 *
 *      read(long uid)：根据 UID 从缓存中获取 DataItem，并校验有效位
 *      insert(long xid, byte[] data)：在 pageIndex 中获取一个足以存储插入内容的页面的页号，
 *                                    获取页面后，首先需要写入插入日志，接着才可以通过 pageX 插入数据，并返回插入位置的偏移。
 *                                    最后需要将页面信息重新插入 pageIndex
 *
 *
 * DM 的所有功能：
 *      1、初始化校验页面1： initPageOne() 和 启动时候进行校验：loadCheckPageOne()
 *      2、读取数据 read(long uid)
 *      3、插入数据 insert(long xid, byte[] data)
 *      4、实现 DataItem 缓存 重写的两个方法： getForCache(long uid)；releaseForCache(DataItem di)
 *      5、为 DataItemImpl.after() 提供的记录更新日志方法：logDataItem(long xid, DataItem di)
 *      6、为 DataItemImpl.release() 提供的释放 DataItem 缓存方法：releaseDataItem(DataItem di)
 *      7、初始化页面索引：fillPageIndex()
 *      8、关闭 DM
 */
public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager {

    TransactionManager tm;
    PageCache pc;
    Logger logger;
    PageIndex pIndex;
    Page pageOne;

    public DataManagerImpl(PageCache pc, Logger logger, TransactionManager tm) {
        super(0);
        this.pc = pc;
        this.logger = logger;
        this.tm = tm;
        this.pIndex = new PageIndex();
    }

    @Override
    public DataItem read(long uid) throws Exception {
        DataItemImpl di = (DataItemImpl)super.get(uid);
        if(!di.isValid()) {
            di.release();
            return null;
        }
        return di;
    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        byte[] raw = DataItem.wrapDataItemRaw(data);
        if(raw.length > PageX.MAX_FREE_SPACE) {
            throw Error.DataTooLargeException;
        }

        PageInfo pi = null;
        for(int i = 0; i < 5; i ++) {
            pi = pIndex.select(raw.length);
            if (pi != null) {
                break;
            } else {
                int newPgno = pc.newPage(PageX.initRaw());
                pIndex.add(newPgno, PageX.MAX_FREE_SPACE);
            }
        }
        if(pi == null) {
            throw Error.DatabaseBusyException;
        }

        Page pg = null;
        int freeSpace = 0;
        try {
            pg = pc.getPage(pi.pgno);
            byte[] log = Recover.insertLog(xid, pg, raw);
            logger.log(log);

            short offset = PageX.insert(pg, raw);

            pg.release();
            return Types.addressToUid(pi.pgno, offset);

        } finally {
            // 将取出的pg重新插入pIndex
            if(pg != null) {
                pIndex.add(pi.pgno, PageX.getFreeSpace(pg));
            } else {
                pIndex.add(pi.pgno, freeSpace);
            }
        }
    }

    @Override
    public void close() {
        super.close();
        logger.close();

        PageOne.setVcClose(pageOne);
        pageOne.release();
        pc.close();
    }

    // 为xid生成update日志
    public void logDataItem(long xid, DataItem di) {
        byte[] log = Recover.updateLog(xid, di);
        logger.log(log);
    }

    public void releaseDataItem(DataItem di) {
        super.release(di.getUid());
    }

    @Override
    protected DataItem getForCache(long uid) throws Exception {
        short offset = (short)(uid & ((1L << 16) - 1));
        uid >>>= 32;
        int pgno = (int)(uid & ((1L << 32) - 1));
        Page pg = pc.getPage(pgno);
        return DataItem.parseDataItem(pg, offset, this);
    }

    @Override
    protected void releaseForCache(DataItem di) {
        di.page().release();
    }

    // 在创建文件时初始化PageOne
    void initPageOne() {
        int pgno = pc.newPage(PageOne.InitRaw());
        assert pgno == 1;
        try {
            pageOne = pc.getPage(pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        pc.flushPage(pageOne);
    }

    // 在打开已有文件时时读入PageOne，并验证正确性
    boolean loadCheckPageOne() {
        try {
            pageOne = pc.getPage(1);
        } catch (Exception e) {
            Panic.panic(e);
        }
        return PageOne.checkVc(pageOne);
    }

    // 初始化pageIndex
    void fillPageIndex() {
        int pageNumber = pc.getPageNumber();
        for(int i = 2; i <= pageNumber; i ++) {
            Page pg = null;
            try {
                pg = pc.getPage(i);
            } catch (Exception e) {
                Panic.panic(e);
            }
            pIndex.add(pg.getPageNumber(), PageX.getFreeSpace(pg));
            pg.release();
        }
    }
    
}
