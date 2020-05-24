package top.fsn.hadoop.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

/***
 *
 * @Author:fsn
 * @Date: 2020/5/24 9:14
 * @Description
 */

@Configuration
public class HadoopConfig {
    @Value("${hdfs.path}")
    private String path;

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }
}
