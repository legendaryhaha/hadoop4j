package top.fsn.hadoop;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/***
 *
 * @Author:fsn
 * @Date: 2020/5/24 8:38
 * @Description
 */

@SpringBootApplication
public class HadoopApplication {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "F:\\hadoop-common-2.2.0-bin-master");
        // 加载库文件
        System.load("F:\\hadoop-common-2.2.0-bin-master\\bin\\hadoop.dll");
        SpringApplication.run(HadoopApplication.class);
    }
}
