package com.springbatch.springbatchtutorial;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableBatchProcessing
@ComponentScan({"com.springbatch.config", "com.springbatch.service", "com.springbatch.listener",
"com.springbatch.processor","com.springbatch.reader","com.springbatch.writer","com.springbatch.controller"
})
@EnableScheduling
public class SpringbatchtutorialApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringbatchtutorialApplication.class, args);
	}

}
