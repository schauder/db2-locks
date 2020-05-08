package de.schauderhaft.db2locks;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.*;
import static org.assertj.core.api.Assertions.*;

@SpringBootTest
class Db2locksApplicationTests {

	List<String> log = new CopyOnWriteArrayList<>();

	@Autowired
	NamedParameterJdbcOperations template;

	@Autowired
	TransactionTemplate tx;

	CountDownLatch firstQueryExecuted = new CountDownLatch(1);
	CountDownLatch firstTransactionFinished = new CountDownLatch(1);
	CountDownLatch secondTransactionFinished = new CountDownLatch(1);

	@BeforeEach
void before() {
		template.update("delete from example", emptyMap());
	}
	@Test
	void contextLoads() {

		String result = template.queryForObject("SELECT 'Alfred' AS NAME FROM SYSIBM.SYSDUMMY1", emptyMap(), String.class);

		assertThat(result).isEqualTo("Alfred");

	}

	@RepeatedTest(10)
	void orchestratedUpdate() throws InterruptedException {

		template.update("insert into example (id, text) values (:id, :text)", parameters(23, "Insert"));

		runInThread(() -> updateAndWait("First Update"));

		runInThread(() -> waitAndUpdate("Second Update"));

		firstQueryExecuted.await(100, TimeUnit.MILLISECONDS);
		log.add("wait");
		Thread.sleep(100);
		firstTransactionFinished.countDown();
		secondTransactionFinished.await(100, TimeUnit.MILLISECONDS);

		assertThat(log).containsExactly("First Update", "wait", "Second Update");
	}

	private void runInThread(SafeRunnable runnable) {

		new Thread(() -> {
			tx.executeWithoutResult(tx -> {
					runnable.run();
			});
		}).start();
	}

	private void updateAndWait(String text) throws InterruptedException {

		template.update("update example set text = :text where id = :id", parameters(23, text));
		log.add(text);
		firstQueryExecuted.countDown();
		firstTransactionFinished.await(100, TimeUnit.MILLISECONDS);
	}

	private void waitAndUpdate(String text) throws InterruptedException {

		firstQueryExecuted.await(100, TimeUnit.MILLISECONDS);
		template.update("update example set text = :text where id = :id", parameters(23, text));
		log.add(text);
		secondTransactionFinished.countDown();
	}


	private Map<String, ?> parameters(int id, String text) {

		HashMap<String, Object> map = new HashMap<>();
		map.put("id", id);
		map.put("text", text);
		return map;
	}


}
