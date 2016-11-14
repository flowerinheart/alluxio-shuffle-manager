package test;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.client.ClientContext;
import alluxio.client.ReadType;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.exception.AlluxioException;

import java.io.IOException;

/**
 * Created by weijia.liu
 * Date :  2016/11/6.
 * Time :  19:32
 */
public class AlluxioTest {

    private static FileSystem fs;

    public static void main(String[] args) throws InterruptedException {
        AlluxioURI uri = new AlluxioURI("alluxio://10.0.2.21:19998");
        Configuration.set(PropertyKey.MASTER_HOSTNAME, uri.getHost());
        Configuration.set(PropertyKey.MASTER_RPC_PORT, Integer.toString(uri.getPort()));
        ClientContext.init();
        fs = FileSystem.Factory.get();

        TestThread t1 = new TestThread();
        TestThread t2 = new TestThread();
        t1.start();
        t2.start();

        Thread.currentThread().join();
    }

    private static class TestThread extends Thread{

        @Override
        public void run() {
            try {
                FileInStream fis = fs.openFile(new AlluxioURI("/sample-1g"), OpenFileOptions.defaults().setReadType(ReadType.NO_CACHE));
                byte[] data = new byte[1024];
                while (fis.read(data) != -1) {
                    System.out.println("Thread " + Thread.currentThread().getId() + " get line is : " + new String(data));
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                fis.close();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (AlluxioException e) {
                e.printStackTrace();
            }
        }
    }
}
