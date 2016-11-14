package test;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.client.ClientContext;
import alluxio.client.file.*;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.exception.AlluxioException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by weijia.liu
 * Date :  2016/11/8.
 * Time :  22:00
 */
public class MultiBlockStreamTest {

    private static FileSystem fs;

    public static void main(String[] args) throws IOException, AlluxioException, InterruptedException {
        AlluxioURI uri = new AlluxioURI("alluxio://10.0.2.21:19998");
        Configuration.set(PropertyKey.MASTER_HOSTNAME, uri.getHost());
        Configuration.set(PropertyKey.MASTER_RPC_PORT, Integer.toString(uri.getPort()));
        Configuration.set(PropertyKey.SECURITY_LOGIN_USERNAME, "skydiscovery");
        ClientContext.init();
        fs = FileSystem.Factory.get();

        //createFiles();
        //listFiles();
        readFiles();
    }

    private static void createFiles() throws IOException, AlluxioException {
        // 分别写文件名为/test/1~4 的大小为44KB的数据，其中每KB的数据依次是1 2 3 4
        for (int i = 1; i <= 4; i++) {
            FileOutStream fos = fs.createFile(new AlluxioURI("/test"+i), CreateFileOptions.defaults().setRecursive(true).setBlockSizeBytes(1024*1024));
            ByteBuffer buffer = ByteBuffer.allocate(4096*1024);
            for (int j = 1; j < 5; j++) {
                for (int k = 0; k < 256*1024; k++) {
                    buffer.putInt(j);
                }
            }
            fos.write(buffer.array());
            fos.close();
        }
        System.out.println("create files over");
    }

    private static void readFiles() throws IOException, InterruptedException {
        List<MultiBlockInfo> list = new ArrayList<MultiBlockInfo>(4);
        list.add(new MultiBlockInfo("gpmaster", 29999, 29998, 24947720192L, 0, 1024*1024));
        list.add(new MultiBlockInfo("gpnode2", 29999, 29998, 24964497409L, 0, 1024*1024));
        list.add(new MultiBlockInfo("gpnode2", 29999, 29998, 24981274626L, 0, 1024*1024));
        list.add(new MultiBlockInfo("gpnode1", 29999, 29998, 24998051843L, 0, 1024*1024));

        MultiBlockInStream stream = ((BaseFileSystem) fs).openMultiBlock(list);
        ByteBuffer buffer = ByteBuffer.allocate(4096*1024);
        stream.read(buffer.array(), 0, 3096*1024);
        stream.read(buffer.array(), 1000, 1000*1024);
        stream.close();
        //buffer.flip();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 1024; i++) {
            sb.append(buffer.getInt());
        }
        System.out.println("str is : " + sb.toString());
    }

    private static void listFiles() throws IOException, AlluxioException {
        List<URIStatus> statusList1 = fs.listStatus(new AlluxioURI("/test1"));
        List<URIStatus> statusList2 = fs.listStatus(new AlluxioURI("/test2"));
        List<URIStatus> statusList3 = fs.listStatus(new AlluxioURI("/test3"));
        List<URIStatus> statusList4 = fs.listStatus(new AlluxioURI("/test4"));
        System.out.println("status 1 : " + statusList1);
        System.out.println("status 2 : " + statusList2);
        System.out.println("status 3 : " + statusList3);
        System.out.println("status 4 : " + statusList4);
    }
}
