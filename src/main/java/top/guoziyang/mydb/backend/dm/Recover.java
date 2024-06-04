package top.guoziyang.mydb.backend.dm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.primitives.Bytes;

import top.guoziyang.mydb.backend.common.SubArray;
import top.guoziyang.mydb.backend.dm.dataItem.DataItem;
import top.guoziyang.mydb.backend.dm.logger.Logger;
import top.guoziyang.mydb.backend.dm.page.Page;
import top.guoziyang.mydb.backend.dm.page.PageX;
import top.guoziyang.mydb.backend.dm.pageCache.PageCache;
import top.guoziyang.mydb.backend.tm.TransactionManager;
import top.guoziyang.mydb.backend.utils.Panic;
import top.guoziyang.mydb.backend.utils.Parser;

/**
 * recover 例程主要也是两步：重做所有已完成事务 redo，撤销所有未完成事务undo：
 * 两个日志格式：
 *      updateLog:
 *      [LogType] [XID] [UID] [OldRaw] [NewRaw]
 *      insertLog:
 *      [LogType] [XID] [Pgno] [Offset] [Raw]
 */
public class Recover {

    private static final byte LOG_TYPE_INSERT = 0;
    private static final byte LOG_TYPE_UPDATE = 1;

    private static final int REDO = 0;
    private static final int UNDO = 1;

    static class InsertLogInfo {
        long xid;
        int pgno;
        short offset;
        byte[] raw;
    }

    static class UpdateLogInfo {
        long xid;
        int pgno;
        short offset;
        byte[] oldRaw;
        byte[] newRaw;
    }

    public static void recover(TransactionManager tm, Logger lg, PageCache pc) {
        System.out.println("Recovering...");

        lg.rewind();
        int maxPgno = 0;
        while(true) {
            byte[] log = lg.next();
            if(log == null) break;
            int pgno;
            if(isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                pgno = li.pgno;
            } else {
                UpdateLogInfo li = parseUpdateLog(log);
                pgno = li.pgno;
            }
            if(pgno > maxPgno) {
                maxPgno = pgno;
            }
        }
        if(maxPgno == 0) {
            maxPgno = 1;
        }
        pc.truncateByBgno(maxPgno);
        System.out.println("Truncate to " + maxPgno + " pages.");

        redoTranscations(tm, lg, pc);
        System.out.println("Redo Transactions Over.");

        undoTranscations(tm, lg, pc);
        System.out.println("Undo Transactions Over.");

        System.out.println("Recovery Over.");
    }

    /**
     * 重做事务
     * @param tm 事务管理器
     * @param lg 日志
     * @param pc 页面缓存
     */
    private static void redoTranscations(TransactionManager tm, Logger lg, PageCache pc) {
        lg.rewind(); // 日志文件指针指向第一条记录
        // 无限循环遍历日志，进行重做操作
        while(true) {
            byte[] log = lg.next();
            if(log == null) break;
            if(isInsertLog(log)) {
                // 如果是一条插入日志，则进行doInsertLog的重做操作
                InsertLogInfo li = parseInsertLog(log);
                long xid = li.xid;
                if(!tm.isActive(xid)) {
                    // 此事务在数据库崩溃的时候必须是非活跃状态，也就是已提交状态才能进行重做
                    doInsertLog(pc, log, REDO);
                }
            } else {
                // 如果是一条更新日志，则进行doUpdateLog的重做操作
                UpdateLogInfo xi = parseUpdateLog(log);
                long xid = xi.xid;
                if(!tm.isActive(xid)) {
                    // 此事务在数据库崩溃的时候必须是非活跃状态，也就是已提交状态才能进行重做
                    doUpdateLog(pc, log, REDO);
                }
            }
        }
    }

    /**
     * 撤销事务
     * @param tm 事务
     * @param lg 日志
     * @param pc 页面缓存
     */
    private static void undoTranscations(TransactionManager tm, Logger lg, PageCache pc) {
        // 日志缓存，key：事务ID，value：日志DATA内容的List；方便后面一次性撤销操作
        Map<Long, List<byte[]>> logCache = new HashMap<>();
        lg.rewind();     // 日志文件指针指向第一条记录
        // 无限循环遍历日志，进行撤销操作
        while(true) {
            byte[] log = lg.next();
            if(log == null) break;
            if(isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);     // 解析为InsertLog格式
                long xid = li.xid;
                if(tm.isActive(xid)) {
                    // 事务处于为提交状态，才能进行撤销
                    if(!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(log);             // 使用一个日志缓存Map记录此事务需要撤销的日志数据
                }
            } else {
                UpdateLogInfo xi = parseUpdateLog(log);
                long xid = xi.xid;
                if(tm.isActive(xid)) {
                    if(!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(log);
                }
            }
        }

        // 对所有active log进行倒序undo，遍历logCache即可
        for(Entry<Long, List<byte[]>> entry : logCache.entrySet()) {
            // 获得一个事务的所有日志List
            List<byte[]> logs = entry.getValue();
            // 倒序进行撤销操作
            for (int i = logs.size()-1; i >= 0; i--) {
                byte[] log = logs.get(i);
                if(isInsertLog(log)) {
                    doInsertLog(pc, log, UNDO);
                } else {
                    doUpdateLog(pc, log, UNDO);
                }
            }
            // 将此事务标记为 abort状态
            tm.abort(entry.getKey());
        }
    }

    /**
     * 判断是否是插入日志
     * insertLog:
     *      [LogType] [XID] [Pgno] [Offset] [Raw]
     * 直接读取日志的第一位状态即可
     * @param log 日志
     */
    private static boolean isInsertLog(byte[] log) {
        return log[0] == LOG_TYPE_INSERT;
    }

    /**
     * updateLog:
     * [LogType] [XID] [UID] [OldRaw] [NewRaw]
     *   1字节    8字节 8字节  8字节
     */
    private static final int OF_TYPE = 0;                       // 日志类型偏移位置
    private static final int OF_XID = OF_TYPE+1;                // 日志的事务ID偏移位置
    private static final int OF_UPDATE_UID = OF_XID+8;          // 日志旧数据的偏移位置
    private static final int OF_UPDATE_RAW = OF_UPDATE_UID+8;   // 日志更新内容的偏移位置

    /**
     * 更新日志
     * @param xid 事务ID
     * @param di 抽象数据
     * @return
     */
    public static byte[] updateLog(long xid, DataItem di) {
        byte[] logType = {LOG_TYPE_UPDATE}; // 创建一个表示日志类型的字节数组，并设置其值为LOG_TYPE_UPDATE
        byte[] xidRaw = Parser.long2Byte(xid); // 将事务ID转换为字节数组
        byte[] uidRaw = Parser.long2Byte(di.getUid()); // 将DataItem对象的唯一标识符转换为字节数组
        byte[] oldRaw = di.getOldRaw(); // 获取DataItem对象的旧原始数据
        SubArray raw = di.getRaw(); // 获取DataItem对象的新原始数据
        byte[] newRaw = Arrays.copyOfRange(raw.raw, raw.start, raw.end); // 将新原始数据转换为字节数组
        return Bytes.concat(logType, xidRaw, uidRaw, oldRaw, newRaw); // 将所有字节数组连接在一起，形成一个完整的更新日志，并返回这个日志
    }

    private static UpdateLogInfo parseUpdateLog(byte[] log) {
        UpdateLogInfo li = new UpdateLogInfo();
        li.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_UPDATE_UID));
        long uid = Parser.parseLong(Arrays.copyOfRange(log, OF_UPDATE_UID, OF_UPDATE_RAW));
        li.offset = (short)(uid & ((1L << 16) - 1));
        uid >>>= 32;
        li.pgno = (int)(uid & ((1L << 32) - 1));
        int length = (log.length - OF_UPDATE_RAW) / 2;
        li.oldRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW, OF_UPDATE_RAW+length);
        li.newRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW+length, OF_UPDATE_RAW+length*2);
        return li;
    }

    private static void doUpdateLog(PageCache pc, byte[] log, int flag) {
        int pgno;
        short offset;
        byte[] raw;
        if(flag == REDO) {
            UpdateLogInfo xi = parseUpdateLog(log);
            pgno = xi.pgno;
            offset = xi.offset;
            raw = xi.newRaw;
        } else {
            UpdateLogInfo xi = parseUpdateLog(log);
            pgno = xi.pgno;
            offset = xi.offset;
            raw = xi.oldRaw;
        }
        Page pg = null;
        try {
            pg = pc.getPage(pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            PageX.recoverUpdate(pg, raw, offset);
        } finally {
            pg.release();
        }
    }

    // [LogType] [XID] [Pgno] [Offset] [Raw]
    private static final int OF_INSERT_PGNO = OF_XID+8;
    private static final int OF_INSERT_OFFSET = OF_INSERT_PGNO+4;
    private static final int OF_INSERT_RAW = OF_INSERT_OFFSET+2;

    /**
     * 插入日志
     * @param xid
     * @param pg
     * @param raw
     * @return
     */
    public static byte[] insertLog(long xid, Page pg, byte[] raw) {
        byte[] logTypeRaw = {LOG_TYPE_INSERT};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] pgnoRaw = Parser.int2Byte(pg.getPageNumber());
        byte[] offsetRaw = Parser.short2Byte(PageX.getFSO(pg));
        return Bytes.concat(logTypeRaw, xidRaw, pgnoRaw, offsetRaw, raw);
    }

    private static InsertLogInfo parseInsertLog(byte[] log) {
        InsertLogInfo li = new InsertLogInfo();
        li.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_INSERT_PGNO));
        li.pgno = Parser.parseInt(Arrays.copyOfRange(log, OF_INSERT_PGNO, OF_INSERT_OFFSET));
        li.offset = Parser.parseShort(Arrays.copyOfRange(log, OF_INSERT_OFFSET, OF_INSERT_RAW));
        li.raw = Arrays.copyOfRange(log, OF_INSERT_RAW, log.length);
        return li;
    }

    private static void doInsertLog(PageCache pc, byte[] log, int flag) {
        InsertLogInfo li = parseInsertLog(log);
        Page pg = null;
        try {
            pg = pc.getPage(li.pgno);
        } catch(Exception e) {
            Panic.panic(e);
        }
        try {
            if(flag == UNDO) {
                DataItem.setDataItemRawInvalid(li.raw);
            }
            PageX.recoverInsert(pg, li.raw, li.offset);
        } finally {
            pg.release();
        }
    }
}
