package top.guoziyang.mydb.backend.dm.logger;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.primitives.Bytes;

import top.guoziyang.mydb.backend.utils.Panic;
import top.guoziyang.mydb.backend.utils.Parser;
import top.guoziyang.mydb.common.Error;

/**
 * 日志文件读写
 *
 * 日志文件标准格式为：
 * [XChecksum] [Log1] [Log2] ... [LogN] [BadTail]
 * XChecksum 4字节 int，为后续所有日志计算的 Checksum(校验和（注意不包括BadTail）)
 * BadTail 是在数据库崩溃时，没有来得及写完的日志数据，这个 BadTail 不一定存在。
 *
 * 每条正确日志[log]的格式为：
 * [Size] [Checksum] [Data]
 * Size: 4字节int 标识 Data 长度
 * Checksum: 4字节 int，单条记录的校验和
 */
public class LoggerImpl implements Logger {

    private static final int SEED = 13331;              // 计算校验和的种子

    private static final int OF_SIZE = 0;               // 每条记录size的起始偏移量
    private static final int OF_CHECKSUM = OF_SIZE + 4; // 每条记录CheckSum的偏移量（Size占用4字节，所以偏移量+4）
    private static final int OF_DATA = OF_CHECKSUM + 4; // 每条记录Data的偏移量（CheckSum占用4字节，所以偏移量+4）

    public static final String LOG_SUFFIX = ".log";     // 日志文件的后缀

    private RandomAccessFile file;
    private FileChannel fc;
    private Lock lock;

    private long position;                              // 当前日志指针的位置
    private long fileSize;                              // 日志文件的大小，初始化时记录，log操作不更新
    private int xChecksum;                              // 日志文件的总 校验和

    LoggerImpl(RandomAccessFile raf, FileChannel fc) {
        this.file = raf;
        this.fc = fc;
        lock = new ReentrantLock();
    }

    LoggerImpl(RandomAccessFile raf, FileChannel fc, int xChecksum) {
        this.file = raf;
        this.fc = fc;
        this.xChecksum = xChecksum;                     // 多了一个总校验和
        lock = new ReentrantLock();
    }

    /**
     * 日志初始化操作，完成日志文件长度的读取、总校验和的读取 和 BadTail的移除
     */
    void init() {
        long size = 0;
        try {
            size = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        if(size < 4) {
            Panic.panic(Error.BadLogFileException);
        }

        // 读取日志文件的总校验和 xChecksum
        ByteBuffer raw = ByteBuffer.allocate(4);
        try {
            fc.position(0);
            fc.read(raw);
        } catch (IOException e) {
            Panic.panic(e);
        }
        int xChecksum = Parser.parseInt(raw.array());
        this.fileSize = size;                           // 文件长度
        this.xChecksum = xChecksum;

        // 检查并移除 badTail
        checkAndRemoveTail();
    }

    /**
     * 在打开一个日志文件时，需要首先校验日志文件的 XChecksum，并移除文件尾部可能存在的 BadTail
     * 由于 BadTail 该条日志尚未写入完成，文件的校验和也就不会包含该日志的校验和，去掉 BadTail 即可保证日志文件的一致性。
     * 检查并移除 badTail
     */
    private void checkAndRemoveTail() {
        // 将指针指向第一条日志记录
        // 将position置为4
        rewind();

        int xCheck = 0;
        while(true) {
            /// internNext会改变position的值，循环结束后position指向最后一个正常[Log]的末尾
            byte[] log = internNext();
            if(log == null) break;
            // 对每条记录都进行计算校验和累加值，就是xChecksum
            xCheck = calChecksum(xCheck, log);
        }
        if(xCheck != xChecksum) {
            Panic.panic(Error.BadLogFileException);
        }

        try {
            /// 将文件截断至position位置，即只保留position前的部分
            truncate(position); // 截断文件到正常日志的末尾，因为前面计算校验和移动了指针，此时的position指向的就是badTail的起始位置
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            // 将文件指针设置至position
            file.seek(position);
        } catch (IOException e) {
            Panic.panic(e);
        }
        // 将position指针指向第一条日志记录，还原指针操作
        rewind();
    }

    /**
     * 单条日志的校验和
     * 其实就是通过一个指定的种子实现的，对日志的每个字节乘一个SEED再累加，就能得到单条日志文件的校验和了。
     * @param xCheck 校验和初始值，一般是0
     * @param log 需要计算校验和的单条日志文件
     * @return 单条日志的校验和
     */
    private int calChecksum(int xCheck, byte[] log) {
        for (byte b : log) {
            xCheck = xCheck * SEED + b;
        }
        return xCheck;
    }

    /**
     * 向日志文件写入日志时，也是首先将数据包裹成日志格式，
     * 写入文件后，再更新文件的校验和，更新校验和时，会刷新缓冲区，保证内容写入磁盘。
     * 写入一条日志记录
     * @param data 日志数据
     */
    @Override
    public void log(byte[] data) {
        byte[] log = wrapLog(data); // 将数据打包成正确的日志格式
        ByteBuffer buf = ByteBuffer.wrap(log);
        lock.lock();
        try {
            fc.position(fc.size()); // 指针移动到文件末尾
            fc.write(buf);          // 将记录写入文件
        } catch(IOException e) {
            Panic.panic(e);
        } finally {
            lock.unlock();
        }
        updateXChecksum(log);       // 更新总校验和
    }

    private void updateXChecksum(byte[] log) {
        this.xChecksum = calChecksum(this.xChecksum, log);
        try {
            fc.position(0);
            fc.write(ByteBuffer.wrap(Parser.int2Byte(xChecksum)));
            fc.force(false);
        } catch(IOException e) {
            Panic.panic(e);
        }
    }

    /**
     * 打包日志文件成一个二进制字节数组
     * @param data
     * @return
     */
    private byte[] wrapLog(byte[] data) {
        byte[] checksum = Parser.int2Byte(calChecksum(0, data));    // 计算单条日志的校验和
        byte[] size = Parser.int2Byte(data.length);                         // 计算data的大小
        return Bytes.concat(size, checksum, data);                          // 打包成标准的单条日志格式，并返回
    }

    /**
     * 截断日志文件，删除x偏移量后面的文件数据
     * @param x 截断位置
     */
    @Override
    public void truncate(long x) throws Exception {
        lock.lock();
        try {
            fc.truncate(x);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 从文件中读取下一条日志，并将其中的 Data 解析出来并返回。
     * Logger 被实现成迭代器模式，通过 next() 方法，不断地从文件中读取下一条日志，并将其中的 Data 解析出来并返回。
     * next() 方法的实现主要依靠 internNext() 。
     * @return
     */
    @Override
    public byte[] next() {
        lock.lock();
        try {
            byte[] log = internNext();
            if(log == null) return null;
            return Arrays.copyOfRange(log, OF_DATA, log.length); // 解析出日志中的DATA数据并返回
        } finally {
            lock.unlock();
        }
    }

    /**
     * 迭代器，获取下一条完整日志记录
     * @return 一条格式完整的完整的日志记录 [Size] [Checksum] [Data]
     */
    private byte[] internNext() {
        /// position是当前日志文件中的指针位置，表示[LogN]的起始，OF_DATA为8，表示一个[LogN]中，[Data]的起始
        if(position + OF_DATA >= fileSize) {
            // 文件指针越界，也就是没有下一条日志了
            return null;
        }

        // 读取单条日志的size
        ByteBuffer tmp = ByteBuffer.allocate(4);
        try {
            fc.position(position);
            fc.read(tmp);   // 将[Size]读到tmp中
        } catch(IOException e) {
            Panic.panic(e);
        }
        int size = Parser.parseInt(tmp.array());

        // size + OF_DATA 是当前position所指[Log]的长度
        if(position + size + OF_DATA > fileSize) {
            return null;
        }

        // 读取 size+checkSum+data
        ByteBuffer buf = ByteBuffer.allocate(OF_DATA + size);
        try {
            fc.position(position);
            fc.read(buf);   // 将[LogN]读到buf中
        } catch(IOException e) {
            Panic.panic(e);
        }
        // 将日志内容打包成二进制数组
        byte[] log = buf.array();

        // 校验 单条日志的 checksum
        /// 根据一条日志的[Data]计算出其校验和
        int checkSum1 = calChecksum(0, Arrays.copyOfRange(log, OF_DATA, log.length)); // 使用data计算checksum
        /// 获取一条日志的[CheckSum]
        int checkSum2 = Parser.parseInt(Arrays.copyOfRange(log, OF_CHECKSUM, OF_DATA));       // 获取日志中记录的checksum
        /// 此[Log]是BadTail
        if(checkSum1 != checkSum2) {
            return null;
        }
        // 文件指针指向下一条日志记录
        position += log.length;
        return log;
    }

    /**
     * 将文件指针指向第一条记录的起始位置
     */
    @Override
    public void rewind() {
        position = 4;
    }

    /**
     * 关闭日志管理器
     */
    @Override
    public void close() {
        try {
            fc.close();
            file.close();
        } catch(IOException e) {
            Panic.panic(e);
        }
    }

}
