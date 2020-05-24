package top.fsn.hadoop.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import top.fsn.hadoop.service.HadoopService;
import top.fsn.hadoop.util.FileUtil;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.List;
import java.util.Map;


/***
 *
 * @Author:fsn
 * @Date: 2020/5/24 9:46
 * @Description
 */

@RestController
public class FileController {
    @Autowired
    private HadoopService hadoopService;

    @PostMapping("/upload")
    public String upload(@RequestParam MultipartFile file) {
        File tmpFile = null;
        try {
            tmpFile = FileUtil.transferToFile(file);
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (hadoopService.upload(tmpFile)) {
            return "200";
        }
        return "500";
    }

    @PostMapping("/list")
    public List<Map<String, String>> list() {
        return hadoopService.list();
    }

    @PostMapping("/download")
    public ResponseEntity<InputStreamResource> download(@RequestParam String name) {
        HttpHeaders headers = new HttpHeaders();
        headers.add("Cache-Control", "no-cache, no-store, must-revalidate");
        headers.add("Content-Disposition", "attachment; filename=" + name);
        headers.add("Pragma", "no-cache");
        headers.add("Expires", "0");
        headers.add("Last-Modified", new Date().toString());
        headers.add("ETag", String.valueOf(System.currentTimeMillis()));
        InputStream inputStream = hadoopService.down(name);
        try {
            return ResponseEntity
                    .ok()
                    .headers(headers)
                    .contentLength(inputStream.available())
                    .contentType(MediaType.parseMediaType("application/octet-stream"))
                    .body(new InputStreamResource(inputStream));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @PostMapping("/count")
    public String count(@RequestParam String name, @RequestParam String target) throws InterruptedException, IOException, ClassNotFoundException {
        if (hadoopService.count(name, target)) {
            return "200";
        }
        return "500";
    }

}
