package com.poc.scsretrydemo;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(OrderController.class)
class OrderControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private OrderProducer orderProducer;

    @Test
    void shouldReturnAcceptedWhenMessageIsPublished() throws Exception {
        when(orderProducer.publish(any(OrderCreatedEvent.class))).thenReturn(true);

        mockMvc.perform(post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "id": 1,
                                  "value": 250,
                                  "status": "CREATED"
                                }
                                """))
                .andExpect(status().isAccepted());
    }

    @Test
    void shouldReturnInternalServerErrorWhenPublishFails() throws Exception {
        when(orderProducer.publish(any(OrderCreatedEvent.class))).thenReturn(false);

        mockMvc.perform(post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "id": 1,
                                  "value": 250,
                                  "status": "CREATED"
                                }
                                """))
                .andExpect(status().isInternalServerError());
    }
}
