# This file is a part of GraknClient.  License is MIT: https://github.com/Humans-of-Julia/GraknClient.jl/blob/main/LICENSE 

# 
# package grakn.client.test.behaviour.connection.session;
# 
# import grakn.client.api.Session;
# import io.cucumber.java.en.Then;
# import io.cucumber.java.en.When;
# 
# import java.util.Iterator;
# import java.util.List;
# import java.util.concurrent.CompletableFuture;
# import java.util.stream.Stream;
# 
# import static grakn.client.api.Session.Type.DATA;
# import static grakn.client.api.Session.Type.SCHEMA;
# import static grakn.client.test.behaviour.connection.ConnectionStepsBase.THREAD_POOL_SIZE;
# import static grakn.client.test.behaviour.connection.ConnectionStepsBase.client;
# import static grakn.client.test.behaviour.connection.ConnectionStepsBase.sessions;
# import static grakn.client.test.behaviour.connection.ConnectionStepsBase.sessionsParallel;
# import static grakn.client.test.behaviour.connection.ConnectionStepsBase.threadPool;
# import static grakn.common.collection.Collections.list;
# import static java.util.Objects.isNull;
# import static org.junit.Assert.assertEquals;
# import static org.junit.Assert.assertTrue;
# 
# public class SessionSteps {
# 
#     @When("connection open schema session for database: {word}")
#     public void connection_open_schema_session_for_database(String name) {
#         connection_open_schema_sessions_for_databases(list(name));
#     }
# 
#     @When("connection open (data )session for database: {word}")
#     public void connection_open_data_session_for_database(String name) {
#         connection_open_data_sessions_for_databases(list(name));
#     }
# 
#     @When("connection open schema session(s) for database(s):")
#     public void connection_open_schema_sessions_for_databases(List<String> names) {
#         for (String name : names) {
#             sessions.add(client.session(name, SCHEMA));
#         }
#     }
# 
#     @When("connection open (data )session(s) for database(s):")
#     public void connection_open_data_sessions_for_databases(List<String> names) {
#         for (String name : names) {
#             sessions.add(client.session(name, DATA));
#         }
#     }
# 
#     @When("connection open (data )sessions in parallel for databases:")
#     public void connection_open_data_sessions_in_parallel_for_databases(List<String> names) {
#         assertTrue(THREAD_POOL_SIZE >= names.size());
# 
#         for (String name : names) {
#             sessionsParallel.add(CompletableFuture.supplyAsync(() -> client.session(name, DATA), threadPool));
#         }
#     }
# 
#     @When("connection close all sessions")
#     public void connection_close_all_sessions() {
#         for (Session session : sessions) {
#             session.close();
#         }
#         sessions.clear();
#     }
# 
#     @Then("session(s) is/are null: {bool}")
#     public void sessions_are_null(Boolean isNull) {
#         for (Session session : sessions) {
#             assertEquals(isNull, isNull(session));
#         }
#     }
# 
#     @Then("session(s) is/are open: {bool}")
#     public void sessions_are_open(Boolean isOpen) {
#         for (Session session : sessions) {
#             assertEquals(isOpen, session.isOpen());
#         }
#     }
# 
#     @Then("sessions in parallel are null: {bool}")
#     public void sessions_in_parallel_are_null(Boolean isNull) {
#         Stream<CompletableFuture<Void>> assertions = sessionsParallel
#                 .stream().map(futureSession -> futureSession.thenApplyAsync(session -> {
#                     assertEquals(isNull, isNull(session));
#                     return null;
#                 }));
# 
#         CompletableFuture.allOf(assertions.toArray(CompletableFuture[]::new)).join();
#     }
# 
#     @Then("sessions in parallel are open: {bool}")
#     public void sessions_in_parallel_are_open(Boolean isOpen) {
#         Stream<CompletableFuture<Void>> assertions = sessionsParallel
#                 .stream().map(futureSession -> futureSession.thenApplyAsync(session -> {
#                     assertEquals(isOpen, session.isOpen());
#                     return null;
#                 }));
# 
#         CompletableFuture.allOf(assertions.toArray(CompletableFuture[]::new)).join();
#     }
# 
#     @Then("session(s) has/have database: {word}")
#     public void sessions_have_database(String name) {
#         sessions_have_databases(list(name));
#     }
# 
#     @Then("session(s) has/have database(s):")
#     public void sessions_have_databases(List<String> names) {
#         assertEquals(names.size(), sessions.size());
#         Iterator<Session> sessionIter = sessions.iterator();
# 
#         for (String name : names) {
#             assertEquals(name, sessionIter.next().database().name());
#         }
#     }
# 
#     @Then("sessions in parallel have databases:")
#     public void sessions_in_parallel_have_databases(List<String> names) {
#         assertEquals(names.size(), sessionsParallel.size());
#         Iterator<CompletableFuture<Session>> futureSessionIter = sessionsParallel.iterator();
#         CompletableFuture[] assertions = new CompletableFuture[names.size()];
# 
#         int i = 0;
#         for (String name : names) {
#             assertions[i++] = futureSessionIter.next().thenApplyAsync(session -> {
#                 assertEquals(name, session.database().name());
#                 return null;
#             });
#         }
# 
#         CompletableFuture.allOf(assertions).join();
#     }
# }
