package top.guoziyang.mydb.backend.dm.logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import top.guoziyang.mydb.backend.utils.Panic;
import top.guoziyang.mydb.backend.utils.Parser;
import top.guoziyang.mydb.common.Error;

// MYDB提供数据库崩溃后的恢复功能，DM 模块在每次对底层数据操作时，都会记录一条日志到磁盘上。
// 在数据库崩溃之后（即第一页校验不对时），再次启动时，可以根据日志的内容，恢复数据文件，保证其一致性。
/**
 * 日志接口
 * 默认提供两个静态函数：
 *      create(String path)：创建日志文件和打开日志功能
 *      open(String path)：打开日志文件和打开日志功能
 */
public interface Logger {
    void log(byte[] data);                  // 写入一条日志记录
    void truncate(long x) throws Exception; // 删除日志文件中x指针后面的文件数据
    byte[] next();                          // logger设计为一个迭代器，next()获取下一条日志，返回的是日志中的DATA数据
    void rewind();                          // 将文件指针指向第一条日志
    void close();                           // 关闭日志

    public static Logger create(String path) {
        File f = new File(path+LoggerImpl.LOG_SUFFIX);
        try {
            if(!f.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (Exception e) {
            Panic.panic(e);
        }
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        ByteBuffer buf = ByteBuffer.wrap(Parser.int2Byte(0));
        try {
            fc.position(0);
            fc.write(buf);
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
        // 打开日志功能，总校验和初始值为0
        return new LoggerImpl(raf, fc, 0);
    }

    public static Logger open(String path) {
        File f = new File(path+LoggerImpl.LOG_SUFFIX);
        if(!f.exists()) {
            Panic.panic(Error.FileNotExistsException);
        }
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        // 打开日志功能，不需要设定总校验和的初始值
        LoggerImpl lg = new LoggerImpl(raf, fc);
        // 日志初始化
        lg.init();

        return lg;
    }
}
