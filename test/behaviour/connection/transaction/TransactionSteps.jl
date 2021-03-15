# This file is a part of GraknClient.  License is MIT: https://github.com/Humans-of-Julia/GraknClient.jl/blob/main/LICENSE 

# 
# package grakn.client.test.behaviour.connection.transaction;
# 
# import grakn.client.api.Session;
# import grakn.client.api.Transaction;
# import graql.lang.Graql;
# import io.cucumber.java.en.Then;
# import io.cucumber.java.en.When;
# import org.hamcrest.Matchers;
# 
# import java.util.ArrayList;
# import java.util.Iterator;
# import java.util.List;
# import java.util.concurrent.CompletableFuture;
# import java.util.function.Consumer;
# 
# import static grakn.client.test.behaviour.connection.ConnectionStepsBase.THREAD_POOL_SIZE;
# import static grakn.client.test.behaviour.connection.ConnectionStepsBase.sessions;
# import static grakn.client.test.behaviour.connection.ConnectionStepsBase.sessionsParallel;
# import static grakn.client.test.behaviour.connection.ConnectionStepsBase.sessionsParallelToTransactionsParallel;
# import static grakn.client.test.behaviour.connection.ConnectionStepsBase.sessionsToTransactions;
# import static grakn.client.test.behaviour.connection.ConnectionStepsBase.sessionsToTransactionsParallel;
# import static grakn.client.test.behaviour.connection.ConnectionStepsBase.threadPool;
# import static grakn.client.test.behaviour.util.Util.assertThrows;
# import static grakn.client.test.behaviour.util.Util.assertThrowsWithMessage;
# import static grakn.common.collection.Collections.list;
# import static java.util.Objects.isNull;
# import static org.junit.Assert.assertEquals;
# import static org.junit.Assert.assertThat;
# import static org.junit.Assert.assertTrue;
# import static org.junit.Assert.fail;
# 
# public class TransactionSteps {
# 
#     // =============================================//
#     // sequential sessions, sequential transactions //
#     // =============================================//
# 
#     @When("(for each )session(,) open(s) transaction(s) of type: {transaction_type}")
#     public void session_opens_transaction_of_type(Transaction.Type type) {
#         for_each_session_open_transactions_of_type(list(type));
#     }
# 
#     @When("(for each )session(,) open transaction(s) of type:")
#     public void for_each_session_open_transactions_of_type(List<Transaction.Type> types) {
#         for (Session session : sessions) {
#             List<Transaction> transactions = new ArrayList<>();
#             for (Transaction.Type type : types) {
#                 Transaction transaction = session.transaction(type);
#                 transactions.add(transaction);
#             }
#             sessionsToTransactions.put(session, transactions);
#         }
#     }
# 
#     @When("(for each )session(,) open transaction(s) of type; throws exception: {transaction_type}")
#     public void for_each_session_open_transactions_of_type_throws_exception(Transaction.Type type) {
#         for_each_session_open_transactions_of_type_throws_exception(list(type));
#     }
# 
#     @Then("(for each )session(,) open transaction(s) of type; throws exception")
#     public void for_each_session_open_transactions_of_type_throws_exception(List<Transaction.Type> types) {
#         for (Session session : sessions) {
#             for (Transaction.Type type : types) {
#                 assertThrows(() -> session.transaction(type));
#             }
#         }
#     }
# 
#     @Then("(for each )session(,) transaction(s) is/are null: {bool}")
#     public void for_each_session_transactions_are_null(boolean isNull) {
#         for_each_session_transactions_are(transaction -> assertEquals(isNull, isNull(transaction)));
#     }
# 
#     @Then("(for each )session(,) transaction(s) is/are open: {bool}")
#     public void for_each_session_transactions_are_open(boolean isOpen) {
#         for_each_session_transactions_are(transaction -> assertEquals(isOpen, transaction.isOpen()));
#     }
# 
#     @Then("transaction commits")
#     public void transaction_commits() {
#         sessionsToTransactions.get(sessions.get(0)).get(0).commit();
#     }
# 
#     @Then("transaction commits; throws exception")
#     public void transaction_commits_throws_exception() {
#         assertThrows(() -> sessionsToTransactions.get(sessions.get(0)).get(0).commit());
#     }
# 
#     @Then("transaction commits; throws exception containing {string}")
#     public void transaction_commits_throws_exception(String exception) {
#         assertThrowsWithMessage(() -> sessionsToTransactions.get(sessions.get(0)).get(0).commit(), exception);
#     }
# 
#     @Then("(for each )session(,) transaction(s) commit(s)")
#     public void for_each_session_transactions_commit() {
#         for (Session session : sessions) {
#             for (Transaction transaction : sessionsToTransactions.get(session)) {
#                 transaction.commit();
#             }
#         }
#     }
# 
#     @Then("(for each )session(,) transaction(s) commit(s); throws exception")
#     public void for_each_session_transactions_commits_throws_exception() {
#         for (Session session : sessions) {
#             for (Transaction transaction : sessionsToTransactions.get(session)) {
#                 assertThrows(transaction::commit);
#             }
#         }
#     }
# 
#     @Then("(for each )session(,) transaction close(s)")
#     public void for_each_session_transaction_closes() {
#         for (Session session : sessions) {
#             for (Transaction transaction : sessionsToTransactions.get(session)) {
#                 transaction.close();
#             }
#         }
#     }
# 
#     private void for_each_session_transactions_are(Consumer<Transaction> assertion) {
#         for (Session session : sessions) {
#             for (Transaction transaction : sessionsToTransactions.get(session)) {
#                 assertion.accept(transaction);
#             }
#         }
#     }
# 
#     @Then("(for each )session(,) transaction(s) has/have type: {transaction_type}")
#     public void for_each_session_transactions_have_type(Transaction.Type type) {
#         for_each_session_transactions_have_type(list(type));
#     }
# 
#     @Then("(for each )session(,) transaction(s) has/have type:")
#     public void for_each_session_transactions_have_type(List<Transaction.Type> types) {
#         for (Session session : sessions) {
#             List<Transaction> transactions = sessionsToTransactions.get(session);
#             assertEquals(types.size(), transactions.size());
# 
#             Iterator<Transaction.Type> typesIterator = types.iterator();
#             Iterator<Transaction> transactionIterator = transactions.iterator();
#             while (typesIterator.hasNext()) {
#                 assertEquals(typesIterator.next(), transactionIterator.next().type());
#             }
#         }
#     }
# 
#     // ===========================================//
#     // sequential sessions, parallel transactions //
#     // ===========================================//
# 
#     @When("for each session, open transaction(s) in parallel of type:")
#     public void for_each_session_open_transactions_in_parallel_of_type(List<Transaction.Type> types) {
#         assertTrue(THREAD_POOL_SIZE >= types.size());
#         for (Session session : sessions) {
#             List<CompletableFuture<Transaction>> transactionsParallel = new ArrayList<>();
#             for (Transaction.Type type : types) {
#                 transactionsParallel.add(CompletableFuture.supplyAsync(() -> session.transaction(type), threadPool));
#             }
#             sessionsToTransactionsParallel.put(session, transactionsParallel);
#         }
#     }
# 
#     @Then("for each session, transactions in parallel are null: {bool}")
#     public void for_each_session_transactions_in_parallel_are_null(boolean isNull) {
#         for_each_session_transactions_in_parallel_are(transaction -> assertEquals(isNull, isNull(transaction)));
#     }
# 
#     @Then("for each session, transactions in parallel are open: {bool}")
#     public void for_each_session_transactions_in_parallel_are_open(boolean isOpen) {
#         for_each_session_transactions_in_parallel_are(transaction -> assertEquals(isOpen, transaction.isOpen()));
#     }
# 
#     private void for_each_session_transactions_in_parallel_are(Consumer<Transaction> assertion) {
#         List<CompletableFuture<Void>> assertions = new ArrayList<>();
#         for (Session session : sessions) {
#             for (CompletableFuture<Transaction> futureTransaction :
#                     sessionsToTransactionsParallel.get(session)) {
# 
#                 assertions.add(futureTransaction.thenApply(transaction -> {
#                     assertion.accept(transaction);
#                     return null;
#                 }));
#             }
#         }
#         CompletableFuture.allOf(assertions.toArray(new CompletableFuture[0])).join();
#     }
# 
#     @Then("for each session, transactions in parallel have type:")
#     public void for_each_session_transactions_in_parallel_have_type(List<Transaction.Type> types) {
#         List<CompletableFuture<Void>> assertions = new ArrayList<>();
#         for (Session session : sessions) {
#             List<CompletableFuture<Transaction>> futureTxs =
#                     sessionsToTransactionsParallel.get(session);
# 
#             assertEquals(types.size(), futureTxs.size());
# 
#             Iterator<Transaction.Type> typesIter = types.iterator();
#             Iterator<CompletableFuture<Transaction>> futureTxsIter = futureTxs.iterator();
# 
#             while (typesIter.hasNext()) {
#                 Transaction.Type type = typesIter.next();
#                 futureTxsIter.next().thenApplyAsync(tx -> {
#                     assertEquals(type, tx.type());
#                     return null;
#                 });
#             }
#         }
# 
#         CompletableFuture.allOf(assertions.toArray(new CompletableFuture[0])).join();
#     }
# 
#     // =========================================//
#     // parallel sessions, parallel transactions //
#     // =========================================//
# 
#     @Then("for each session in parallel, transactions in parallel are null: {bool}")
#     public void for_each_session_in_parallel_transactions_in_parallel_are_null(boolean isNull) {
#         for_each_session_in_parallel_transactions_in_parallel_are(transaction -> assertEquals(isNull, isNull(transaction)));
#     }
# 
#     @Then("for each session in parallel, transactions in parallel are open: {bool}")
#     public void for_each_session_in_parallel_transactions_in_parallel_are_open(boolean isOpen) {
#         for_each_session_in_parallel_transactions_in_parallel_are(transaction -> assertEquals(isOpen, transaction.isOpen()));
#     }
# 
#     private void for_each_session_in_parallel_transactions_in_parallel_are(Consumer<Transaction> assertion) {
#         List<CompletableFuture<Void>> assertions = new ArrayList<>();
#         for (CompletableFuture<Session> futureSession : sessionsParallel) {
#             for (CompletableFuture<Transaction> futureTransaction : sessionsParallelToTransactionsParallel.get(futureSession)) {
#                 assertions.add(futureTransaction.thenApply(transaction -> {
#                     assertion.accept(transaction);
#                     return null;
#                 }));
#             }
#         }
#         CompletableFuture.allOf(assertions.toArray(new CompletableFuture[0])).join();
#     }
# 
# 
#     // ===================================//
#     // transaction behaviour with queries //
#     // ===================================//
# 
#     @Then("for each transaction, define query; throws exception containing {string}")
#     public void for_each_transaction_execute_define_throws_exception(String expectedException, String defineQueryStatements) {
#         for (Session session : sessions) {
#             for (Transaction transaction : sessionsToTransactions.get(session)) {
#                 try {
#                     transaction.query().define(Graql.parseQuery(defineQueryStatements).asDefine()).get();
#                     fail();
#                 } catch (Exception e) {
#                     assertThat(e.getMessage(), Matchers.containsString(expectedException));
#                 }
#             }
#         }
#     }
# }
