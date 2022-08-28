package com.marcolotz.db2parquet;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages="com.marcolotz")
public class Db2parquetApplication {

	public static void main(String[] args) {
		SpringApplication.run(Db2parquetApplication.class, args);
	}

}
