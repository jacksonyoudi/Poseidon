package com.youdi.sponebob.controller;


import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
public class LoggerController {

    @Autowired
    private KafkaTemplate<String, String> KafkaTemplate;


    @RequestMapping("test")
    public String test() {
        System.out.println("test");
        return "sucess";
    }

    @RequestMapping("test2")
    public String test2(
            @RequestParam("name") String nn,
            @RequestParam(value = "age", defaultValue = "18") int age
    ) {
        System.out.println(nn + ":" + age);
        return nn + age;
    }


    @RequestMapping("applog")
    public String getLog(
            @RequestParam("param") String jsonStr
    ) {

        // 数据落盘
        log.info(jsonStr);


        // 数据写入kafka
        KafkaTemplate.send("ods_base_log", jsonStr);


        return "success";

    }

}
