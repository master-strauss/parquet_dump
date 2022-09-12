package com.marcolotz.db2parquet.web;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
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

    // When
    mockMvc.perform(put("/v1/trigger")).andExpect(status().isAccepted());

    // Then
    verify(ingestionService).triggerIngestion();
  }

  @Test
  @DisplayName( "Then exceptions in the ingestions are not returned to the client" )
  void whenExceptionIsRaised_ClientResponseContainsNoInformation() throws Exception {
    // Given
    doThrow(new RuntimeException("booom!")).when(ingestionService).triggerIngestion();

    // Expect
    mockMvc.perform(put("/v1/trigger")).andExpect(status().isAccepted());
  }

  @Test
  @DisplayName( "Then ingestions cannot be triggered when an ingestion is running" )
  void whenIngestionIsHappening_thenServiceReturnsBusyStats() throws Exception {
    // Given
    doReturn(true).when(ingestionService).isBusy();

    // When
    mockMvc.perform(put("/v1/trigger")).andExpect(status().is(503));

    // Then
    verify(ingestionService, never()).triggerIngestion();
  }
}