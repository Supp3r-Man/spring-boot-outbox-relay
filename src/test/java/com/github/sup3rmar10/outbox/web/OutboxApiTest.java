package com.github.sup3rmar10.outbox.web;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;

/** The full Spring application on MockMvc with the background scheduler off; polls are triggered explicitly. */
@SpringBootTest(properties = {
        "outbox.scheduler-enabled=false",
        "outbox.max-attempts=2",
        "outbox.base-backoff-millis=0",
        "outbox.max-backoff-millis=0",
        "spring.datasource.url=jdbc:h2:mem:webtest;DB_CLOSE_DELAY=-1;LOCK_TIMEOUT=10000"})
@AutoConfigureMockMvc
class OutboxApiTest {

    @Autowired MockMvc mvc;
    @Autowired JdbcTemplate jdbc;
    @Autowired ObjectMapper json;

    @BeforeEach
    void clean() throws Exception {
        jdbc.update("DELETE FROM outbox");
        jdbc.update("DELETE FROM payment");
        jdbc.update("DELETE FROM processed_message");
        jdbc.update("DELETE FROM account_captured");
        mvc.perform(post("/demo/broker/fail").param("count", "0")).andExpect(status().isOk());
    }

    private String createPayment(String account, long amount) throws Exception {
        String body = mvc.perform(post("/payments").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"accountId\":\"" + account + "\",\"amountMinor\":" + amount + ",\"currency\":\"USD\"}"))
                .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString();
        return json.readTree(body).get("id").asText();
    }

    private JsonNode poll() throws Exception {
        return json.readTree(mvc.perform(post("/admin/relay/poll")).andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString());
    }

    private ResultActions metrics() throws Exception {
        return mvc.perform(get("/outbox/metrics")).andExpect(status().isOk());
    }

    @Test
    void creatingAPaymentReturns201WithLocationAndTheFirstEventSequence() throws Exception {
        mvc.perform(post("/payments").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"accountId\":\"acct-1\",\"amountMinor\":2500,\"currency\":\"USD\"}"))
                .andExpect(status().isCreated())
                .andExpect(header().string("Location", containsString("/payments/pmt-")))
                .andExpect(jsonPath("$.status").value("CREATED"))
                .andExpect(jsonPath("$.eventSeq").value(1))
                .andExpect(jsonPath("$.amountMinor").value(2500));
    }

    @Test
    void invalidBodiesListEveryBadFieldAsProblemJson() throws Exception {
        mvc.perform(post("/payments").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"accountId\":\"\",\"amountMinor\":0,\"currency\":\"usd\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith("application/problem+json"))
                .andExpect(jsonPath("$.code").value("validation"))
                .andExpect(jsonPath("$.errors", hasSize(3)))
                .andExpect(jsonPath("$.errors[0].field").value("accountId"))
                .andExpect(jsonPath("$.errors[1].field").value("amountMinor"))
                .andExpect(jsonPath("$.errors[2].field").value("currency"));
    }

    @Test
    void malformedJsonIs400() throws Exception {
        mvc.perform(post("/payments").contentType(MediaType.APPLICATION_JSON).content("{nope"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void anUnknownPaymentIs404WithACode() throws Exception {
        mvc.perform(get("/payments/pmt-missing")).andExpect(status().isNotFound())
                .andExpect(jsonPath("$.code").value("not-found"));
    }

    @Test
    void captureThenRefundWalkTheLifecycleAndRepeatsAre409() throws Exception {
        String id = createPayment("acct-1", 500);
        mvc.perform(post("/payments/" + id + "/refund")).andExpect(status().isConflict());
        mvc.perform(post("/payments/" + id + "/capture")).andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("CAPTURED")).andExpect(jsonPath("$.eventSeq").value(2));
        mvc.perform(post("/payments/" + id + "/capture")).andExpect(status().isConflict())
                .andExpect(jsonPath("$.code").value("conflict"));
        mvc.perform(post("/payments/" + id + "/refund")).andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("REFUNDED")).andExpect(jsonPath("$.eventSeq").value(3));
        mvc.perform(get("/payments/" + id)).andExpect(status().isOk()).andExpect(jsonPath("$.status").value("REFUNDED"));
    }

    @Test
    void metricsShowPendingRowsBeforeAPollAndPublishedRowsAfter() throws Exception {
        createPayment("acct-1", 100);
        createPayment("acct-2", 200);
        metrics().andExpect(jsonPath("$.pending").value(2)).andExpect(jsonPath("$.published").value(0))
                .andExpect(jsonPath("$.dead").value(0)).andExpect(jsonPath("$.inFlight").value(0))
                .andExpect(jsonPath("$.oldestUnpublishedAgeMillis").isNumber());
        JsonNode result = poll();
        org.junit.jupiter.api.Assertions.assertEquals(2, result.get("published").asInt());
        metrics().andExpect(jsonPath("$.pending").value(0)).andExpect(jsonPath("$.published").value(2))
                .andExpect(jsonPath("$.oldestUnpublishedAgeMillis").doesNotExist());
    }

    @Test
    void pollDeliversToTheBrokerAndTheSampleConsumerAppliesTheCapture() throws Exception {
        String id = createPayment("acct-7", 4200);
        mvc.perform(post("/payments/" + id + "/capture")).andExpect(status().isOk());
        poll();
        poll();
        mvc.perform(get("/demo/broker/messages")).andExpect(jsonPath("$[?(@.idempotencyKey=='" + id + ":1')]", hasSize(1)))
                .andExpect(jsonPath("$[?(@.idempotencyKey=='" + id + ":2')]", hasSize(1)));
        mvc.perform(get("/demo/consumer")).andExpect(jsonPath("$.processedMessages").value(2))
                .andExpect(jsonPath("$.eventsApplied").value(1))
                .andExpect(jsonPath("$.netCapturedMinorByAccount.acct-7").value(4200));
    }

    @Test
    void aFailingBrokerLeadsToRetryThenDeadThenRequeueThenDelivery() throws Exception {
        String id = createPayment("acct-1", 100);
        mvc.perform(post("/demo/broker/fail").param("count", "2")).andExpect(jsonPath("$.pendingFailures").value(2));
        org.junit.jupiter.api.Assertions.assertEquals(1, poll().get("retried").asInt());
        org.junit.jupiter.api.Assertions.assertEquals(1, poll().get("dead").asInt());
        metrics().andExpect(jsonPath("$.dead").value(1)).andExpect(jsonPath("$.pending").value(0));

        String listed = mvc.perform(get("/admin/outbox"))
                .andExpect(status().isOk()).andExpect(jsonPath("$", hasSize(1)))
                .andExpect(jsonPath("$[0].status").value("DEAD"))
                .andExpect(jsonPath("$[0].idempotencyKey").value(id + ":1"))
                .andExpect(jsonPath("$[0].lastError").value(containsString("broker unavailable")))
                .andReturn().getResponse().getContentAsString();
        long rowId = json.readTree(listed).get(0).get("id").asLong();

        mvc.perform(post("/admin/outbox/" + rowId + "/requeue")).andExpect(status().isOk())
                .andExpect(jsonPath("$.requeued").value(1));
        org.junit.jupiter.api.Assertions.assertEquals(1, poll().get("published").asInt());
        metrics().andExpect(jsonPath("$.dead").value(0)).andExpect(jsonPath("$.published").value(1));
    }

    @Test
    void requeueAllMovesEveryDeadRow() throws Exception {
        createPayment("acct-1", 100);
        createPayment("acct-2", 100);
        mvc.perform(post("/demo/broker/fail").param("count", "4"));
        poll();
        poll();
        metrics().andExpect(jsonPath("$.dead").value(2));
        mvc.perform(post("/admin/outbox/dead/requeue")).andExpect(jsonPath("$.requeued").value(2));
        mvc.perform(post("/admin/outbox/dead/requeue")).andExpect(jsonPath("$.requeued").value(0));
        org.junit.jupiter.api.Assertions.assertEquals(2, poll().get("published").asInt());
    }

    @Test
    void requeueingARowThatIsNotDeadIs409AndAnUnknownRowIs404() throws Exception {
        createPayment("acct-1", 100);
        long rowId = jdbc.queryForObject("SELECT id FROM outbox", Long.class);
        mvc.perform(post("/admin/outbox/" + rowId + "/requeue")).andExpect(status().isConflict())
                .andExpect(jsonPath("$.code").value("conflict"));
        mvc.perform(post("/admin/outbox/999999/requeue")).andExpect(status().isNotFound());
    }

    @Test
    void adminListValidatesItsParameters() throws Exception {
        mvc.perform(get("/admin/outbox").param("status", "BOGUS")).andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("validation"));
        mvc.perform(get("/admin/outbox").param("limit", "0")).andExpect(status().isBadRequest());
        mvc.perform(get("/admin/outbox").param("limit", "501")).andExpect(status().isBadRequest());
        createPayment("acct-1", 100);
        mvc.perform(get("/admin/outbox").param("status", "new")).andExpect(jsonPath("$", hasSize(1)));
    }

    @Test
    void pollingAnEmptyOutboxReturnsZeros() throws Exception {
        JsonNode r = poll();
        org.junit.jupiter.api.Assertions.assertEquals(0, r.get("claimed").asInt());
    }

    @Test
    void theDemoFailEndpointRejectsNegativeCounts() throws Exception {
        mvc.perform(post("/demo/broker/fail").param("count", "-1")).andExpect(status().isBadRequest());
    }
}
