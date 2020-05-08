package de.schauderhaft.db2locks;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.RepeatedTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.List;
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
		template.update("insert into example (id, text) values (23, 'Insert')", emptyMap());
	}


	@RepeatedTest(10)
	@Disabled
	void noopDoesNotLock() throws InterruptedException {

		Runnable obtainLock = () -> {
		};

		checkLockFails(obtainLock);
	}

	@RepeatedTest(10)
	void selectDoesNotLock() throws InterruptedException {

		Runnable obtainLock = () ->
				template.getJdbcOperations().queryForObject("select 1 from example where id = 23", Long.class);

		checkLockFails(obtainLock);
	}

	@RepeatedTest(10)
	void selectForUpdateDoesNotLock() throws InterruptedException {

		Runnable obtainLock = () ->
				template.getJdbcOperations().queryForObject("select id from example where id = 23 for update", Long.class);

		checkLockFails(obtainLock);
	}

	@RepeatedTest(10)
	void selectTextForUpdateDoesNotLock() throws InterruptedException {

		Runnable obtainLock = () ->
				template.getJdbcOperations().queryForObject("select text from example where id = 23 for update", String.class);

		checkLockFails(obtainLock);
	}

	// ↧↧↧↧↧ things that work ↧↧↧↧↧

	@RepeatedTest(10)
	void updateDoesLock() throws InterruptedException {

		Runnable obtainLock = () ->
				template.update("update example set text = 'First Update' where id = 23", emptyMap());

		checkLock(obtainLock);
	}

	@RepeatedTest(10)
	void selectTextForKeepUpdateLocksDoesLock() throws InterruptedException {

		Runnable obtainLock = () ->
				template.getJdbcOperations().queryForObject("select text from example where id = 23 for update with rs use and keep update locks", String.class);

		checkLock(obtainLock);
	}

	@RepeatedTest(10)
	void selectIdForKeepUpdateLocksDoesLock() throws InterruptedException {

		Runnable obtainLock = () ->
				template.getJdbcOperations().queryForObject("select id from example where id = 23 for update with rs use and keep update locks", Integer.class);

		checkLock(obtainLock);
	}

	@RepeatedTest(10)
	void selectConsForKeepUpdateLocksDoesLock() throws InterruptedException {

		Runnable obtainLock = () ->
				template.getJdbcOperations().queryForObject("select 23 from example where id = 23 for update with rs use and keep update locks", Integer.class);

		checkLock(obtainLock);
	}

	@RepeatedTest(10)
	void executeWithParameterDoesNotLock() throws InterruptedException {

		Runnable obtainLock = () ->
				template.execute("select id from example where id = 23 for update with rs use and keep update locks", emptyMap(), ps -> {ps.execute(); return null;});

		checkLockFails(obtainLock);
	}

	@RepeatedTest(10)
	void executeDoesNotWork() throws InterruptedException {

		Runnable obtainLock = () ->
				template.getJdbcOperations().execute("select id from example where id = 23 for update with rs use and keep update locks");

		checkLockFails(obtainLock);
	}



	private void checkLock(Runnable runnable) throws InterruptedException {
		doTestRun(runnable);

		assertThat(log).containsExactly("Obtain Lock", "wait", "Second Update");
	}

	private void checkLockFails(Runnable runnable) throws InterruptedException {
		doTestRun(runnable);

		assertThat(log).containsExactly("Obtain Lock", "Second Update", "wait");
	}

	private void doTestRun(Runnable runnable) throws InterruptedException {
		runInThread(() -> executeAndWait(runnable, "Obtain Lock"));

		runInThread(() -> waitAndUpdate("Second Update"));

		firstQueryExecuted.await(100, TimeUnit.MILLISECONDS);
		Thread.sleep(100);
		log.add("wait");
		firstTransactionFinished.countDown();
		secondTransactionFinished.await(100, TimeUnit.MILLISECONDS);
	}

	private void runInThread(SafeRunnable runnable) {

		new Thread(() -> {
			tx.executeWithoutResult(tx -> {
				runnable.run();
			});
		}).start();
	}

	private void executeAndWait(Runnable runnable, String text) throws InterruptedException {

		runnable.run();
		log.add(text);
		firstQueryExecuted.countDown();
		firstTransactionFinished.await(100, TimeUnit.MILLISECONDS);
	}

	private void waitAndUpdate(String text) throws InterruptedException {

		firstQueryExecuted.await(100, TimeUnit.MILLISECONDS);
		template.update("update example set text = '" + text + "' where id = 23", emptyMap());
		log.add(text);
		secondTransactionFinished.countDown();
	}
}
