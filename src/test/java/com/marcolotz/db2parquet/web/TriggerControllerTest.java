package com.marcolotz.db2parquet.web;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.marcolotz.db2parquet.port.IngestionService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

@WebMvcTest( controllers = TriggerController.class )
@DisplayName( "When using the REST API to trigger ingestions" )
// Keep in mind that ComponentScan doesn't play well with WebMvcTest: https://tinyurl.com/27nn5yws
class TriggerControllerTest {

  @MockBean
  private IngestionService ingestionService;
  @Autowired
  private MockMvc mockMvc;

  @BeforeEach
  void setup() {
    reset(ingestionService);
  }

  @Test
  @DisplayName( "Then ingestions can be triggered" )
  void whenRequestIsSent_theIngestionIsTriggered() throws Exception {
    // Given
    doNothing().when(ingestionService).triggerIngestion();

    final MvcResult result = mockMvc.perform(put("/v1/trigger")).andReturn();

    // When
    mockMvc.perform(asyncDispatch(result)).andExpect(status().isOk());

    // Then
    verify(ingestionService).triggerIngestion();
  }

  @Test
  @DisplayName( "Then exceptions in the ingestions are filtered out from response" )
  void whenExceptionIsRaised_ClientResponseContainsNoInformation() throws Exception {
    // Given
    doThrow(new RuntimeException("booom!")).when(ingestionService).triggerIngestion();

    final MvcResult result = mockMvc.perform(put("/v1/trigger")).andReturn();

    // When
    mockMvc.perform(asyncDispatch(result)).andExpect(status().is5xxServerError());

    // Then
    verify(ingestionService).triggerIngestion();
  }
}