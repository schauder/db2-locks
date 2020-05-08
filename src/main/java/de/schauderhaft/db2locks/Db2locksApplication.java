package de.schauderhaft.db2locks;

import org.awaitility.Awaitility;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.DataSourceInitializer;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.testcontainers.containers.Db2Container;

import javax.sql.DataSource;
import java.sql.Connection;

@SpringBootApplication
public class Db2locksApplication {

	public static void main(String[] args) {
		SpringApplication.run(Db2locksApplication.class, args);
	}


	@Bean
	DataSource dataSource() {

		Db2Container db2Container = new Db2Container().withReuse(true);
		db2Container.start();
		DriverManagerDataSource dataSource = new DriverManagerDataSource(db2Container.getJdbcUrl(), db2Container.getUsername(), db2Container.getPassword());
		Awaitility.await().untilAsserted(() -> {
			try (Connection ignored = dataSource.getConnection()) {
			}
		});
		return dataSource;
	}

	@Bean
	DataSourceInitializer dataSourceInitializer(DataSource dataSource) {

		DataSourceInitializer initializer = new DataSourceInitializer();
		initializer.setDataSource(dataSource);


		ClassPathResource script = new ClassPathResource("schema.sql");
		ResourceDatabasePopulator populator = new ResourceDatabasePopulator(script);
		populator.setIgnoreFailedDrops(true);
		initializer.setDatabasePopulator(populator);

		return initializer;
	}
}
