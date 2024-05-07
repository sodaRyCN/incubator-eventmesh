package com.apache.eventmesh.admin.server;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "com.apache.eventmesh.admin.server")
public class ExampleAdminServer {
    public static void main(String[] args) {
        SpringApplication.run(ExampleAdminServer.class);
    }
}
