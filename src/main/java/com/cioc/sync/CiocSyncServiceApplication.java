package com.cioc.sync;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class CiocSyncServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(CiocSyncServiceApplication.class, args);
	}

}
