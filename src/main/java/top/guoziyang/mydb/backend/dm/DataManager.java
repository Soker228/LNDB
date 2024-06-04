package top.guoziyang.mydb.backend.dm;

import top.guoziyang.mydb.backend.dm.dataItem.DataItem;
import top.guoziyang.mydb.backend.dm.logger.Logger;
import top.guoziyang.mydb.backend.dm.page.PageOne;
import top.guoziyang.mydb.backend.dm.pageCache.PageCache;
import top.guoziyang.mydb.backend.tm.TransactionManager;

/**
 * 数据管理模块接口：
 * 默认提供两个静态功能： 新建数据管理模块 和 打开数据管理器
 *  create(String path, long mem, TransactionManager tm)：
 *  open(String path, long mem, TransactionManager tm)：
 */
public interface DataManager {
    DataItem read(long uid) throws Exception;                   // 读取数据
    long insert(long xid, byte[] data) throws Exception;        // 插入数据
    void close();                                               // 关闭数据管理器

    //从空文件创建首先需要对第一页进行初始化
    // 静态方法，用于创建DataManager实例
    public static DataManager create(String path, long mem, TransactionManager tm) {
        // 创建一个PageCache实例，path是文件路径，mem是内存大小
        PageCache pc = PageCache.create(path, mem);             // 新建页面缓存
        // 创建一个Logger实例，path是文件路径
        Logger lg = Logger.create(path);                        // 新建日志

        // 创建一个DataManagerImpl实例，pc是PageCache实例，lg是Logger实例，tm是TransactionManager实例
        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);   // 新建数据管理器
        dm.initPageOne();                                       // 初始化校验页面1
        return dm;                                              // 返回创建的DataManagerImpl实例
    }


    // 静态方法，用于打开已存在的DataManager实例
    public static DataManager open(String path, long mem, TransactionManager tm) {
        PageCache pc = PageCache.open(path, mem);               // 打开页面缓存
        Logger lg = Logger.open(path);                          // 打开日志
        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);   // 打开数据管理器
        if(!dm.loadCheckPageOne()) {
            // 校验页面1错误，说明数据库非正常关闭，需要进行崩溃恢复
            Recover.recover(tm, lg, pc);
        }
        dm.fillPageIndex();                                     // 重新填写页面索引
        PageOne.setVcOpen(dm.pageOne);                          // 重新填写 校验页面1
        dm.pc.flushPage(dm.pageOne);                            // 将校验页面1 强行写入数据源

        return dm;
    }
}
