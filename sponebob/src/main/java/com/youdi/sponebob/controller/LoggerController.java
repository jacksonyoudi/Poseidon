package com.youdi.sponebob.controller;


import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

//@Controller
@RestController
public class LoggerController {

    @RequestMapping("test")
//    @RequestBody
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

}
