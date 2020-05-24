package top.fsn.hadoop.util;

import org.springframework.web.multipart.MultipartFile;

import java.io.*;

/***
 *
 * @Author:fsn
 * @Date: 2020/5/24 10:02
 * @Description
 */


public class FileUtil {
    public static File transferToFile(MultipartFile file) throws IOException {
        File tmpFile = null;
        if (file == null || file.getSize() <= 0 ||
                file.getOriginalFilename() == null) {
            return null;
        }
        InputStream inputStream = file.getInputStream();
        tmpFile = new File(file.getOriginalFilename());
        OutputStream outputStream = new FileOutputStream(tmpFile);
        byte[] buffer = new byte[1024];
        int read = 0;
        while ((read = inputStream.read(buffer, 0 , 1024)) != -1) {
            outputStream.write(buffer, 0, read);
        }
        outputStream.close();
        inputStream.close();
        return tmpFile;
    }
}
