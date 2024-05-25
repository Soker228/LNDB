package top.guoziyang.mydb.backend.dm.page;

import java.util.Arrays;

import top.guoziyang.mydb.backend.dm.pageCache.PageCache;
import top.guoziyang.mydb.backend.utils.RandomUtil;

/**
 * 特殊管理第一页
 * 数据库文件的第一页，通常用作一些特殊用途，比如存储一些元数据，用来启动检查什么的.
 * LNDB 的第一页，只是用来做启动检查。
 * ValidCheck 校验页面，唯一作用就是校验数据库是否正常关闭
 * db启动时给100~107字节处填入一个随机字节，db关闭时将其拷贝到108~115字节
 * 数据库在每次启动时，就会检查第一页两处的字节是否相同，以此来判断上一次是否正常关闭。
 * 如果是异常关闭，就需要执行数据的恢复流程。
 */
public class PageOne {
    private static final int OF_VC = 100;
    private static final int LEN_VC = 8;

    /**
     * 初始化一个页面
     * @return 一个设置了100~107字节处随机数的特殊页面1
     */
    public static byte[] InitRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE]; // 新建一个数据页大小的字节数组
        setVcOpen(raw); // 调用setVcOpen(byte[] raw) 在100~107字节处填入一个8位的随机数
        return raw;
    }

    public static void setVcOpen(Page pg) {
        pg.setDirty(true);
        setVcOpen(pg.getData());
    }

    /**
     * 数据库启动的时候，在raw文件指定位置填入一个指定大小的随机数
     * @param raw 字节数组
     */
    private static void setVcOpen(byte[] raw) {
        // 将随机数拷贝至raw中的第100-107字节
        System.arraycopy(RandomUtil.randomBytes(LEN_VC), 0, raw, OF_VC, LEN_VC);
    }

    // 关闭时拷贝字节
    public static void setVcClose(Page pg) {
        pg.setDirty(true);
        setVcClose(pg.getData());
    }

    /**
     * 数据库关闭的时候，把raw文件中100~107字节处的内容拷贝到108~115字节处
     * @param raw
     */
    private static void setVcClose(byte[] raw) {
        System.arraycopy(raw, OF_VC, raw, OF_VC+LEN_VC, LEN_VC);
    }

    // 校验字节
    public static boolean checkVc(Page pg) {
        return checkVc(pg.getData());
    }

    /**
     * 验证raw文件中100~107字节处的内容和108~115字节处内容是否一致
     * @param raw
     * @return
     */
    private static boolean checkVc(byte[] raw) {
        return Arrays.equals(Arrays.copyOfRange(raw, OF_VC, OF_VC+LEN_VC), Arrays.copyOfRange(raw, OF_VC+LEN_VC, OF_VC+2*LEN_VC));
    }
}