package com.zjtd.publisher;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.zjtd.publisher.mapper")
public class RealtimePublisherApplication {

    public static void main(String[] args) {
        SpringApplication.run(RealtimePublisherApplication.class, args);
    }

}
