package com.handson.siteIndexer;

import com.handson.siteIndexer.configs.KafkaEmbeddedConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.kafka.test.rule.KafkaEmbedded;

@SpringBootApplication
public class SiteIndexerApplication {

	public static void main(String[] args) {
		SpringApplication.run(SiteIndexerApplication.class, args);
	}

	public static final KafkaEmbedded KAFKA_EMBEDDED = createKafkaEmbedded();
	private static KafkaEmbedded createKafkaEmbedded() {
		AnnotationConfigApplicationContext context =
				new AnnotationConfigApplicationContext(KafkaEmbeddedConfig.class);
		KafkaEmbedded kafkaEmbedded = context.getBean(KafkaEmbedded.class);
		Runtime.getRuntime().addShutdownHook(new Thread(context::close));
		return kafkaEmbedded;
	}
}
