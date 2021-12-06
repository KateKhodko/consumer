package com.khodko.kafka.consumer.service;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.gridfs.GridFsTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;

@Service
@Slf4j
public class HistoryService {

    private final GridFsTemplate template;

    @Autowired
    public HistoryService(GridFsTemplate template) {
        this.template = template;
    }

    @KafkaListener(topics = "request", groupId = "server.broadcast")
    public void listenGroup(HttpServletRequest httpServletRequest) {
        log.info("=> consumed {}", httpServletRequest.toString());
    }

    public String addFile(MultipartFile upload) throws IOException {
        DBObject metadata = new BasicDBObject();
        metadata.put("fileSize", upload.getSize());

        ObjectId fileID = template.store(upload.getInputStream(), upload.getOriginalFilename(), upload.getContentType(), metadata);

        return fileID.toString();
    }
}
