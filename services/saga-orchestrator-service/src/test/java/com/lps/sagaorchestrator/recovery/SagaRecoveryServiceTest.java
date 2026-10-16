package com.lps.sagaorchestrator.recovery;

import com.lps.sagaorchestrator.outbox.SagaOutboxDiagnosticRow;
import com.lps.sagaorchestrator.outbox.SagaOutboxRepository;
import com.lps.sagaorchestrator.outbox.SagaOutboxStatus;
import com.lps.sagaorchestrator.saga.SagaInstance;
import com.lps.sagaorchestrator.saga.SagaRepository;
import com.lps.sagaorchestrator.saga.SagaStatus;
import com.lps.sagaorchestrator.saga.SagaStep;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.SliceImpl;

import java.util.List;
import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/** Unit tests distinguish operational diagnostics from mutations or business retries. */
class SagaRecoveryServiceTest {
    private final SagaRepository sagas = mock(SagaRepository.class);
    private final SagaOutboxRepository outbox = mock(SagaOutboxRepository.class);
    private final SagaRecoveryService recovery = new SagaRecoveryService(sagas, outbox, 2, 10);

    @ParameterizedTest
    @EnumSource(SagaOutboxStatus.class)
    void classifiesEveryOutboxStatusWithoutPublishing(SagaOutboxStatus status) {
        SagaInstance saga = saga(SagaStep.BUREAU);
        SagaOutboxDiagnosticRow command = command(saga.getApplicationId(), "PerformBureauCheck", status);
        var expected = switch (status) {
            case NEW -> SagaRecoveryClassification.WAITING_FOR_PUBLICATION;
            case CLAIMED -> SagaRecoveryClassification.PUBLICATION_IN_PROGRESS;
            case PUBLISHED -> SagaRecoveryClassification.AWAITING_COMPLETION;
            case FAILED -> SagaRecoveryClassification.PUBLICATION_FAILED;
        };
        givenSinglePage(List.of(saga), List.of(command));

        SagaRecoverySummary result = recovery.inspect();
        assertEquals(1, result.inspected());
        assertEquals(1, result.count(expected));
        verify(sagas, never()).save(any());
        verify(outbox, never()).save(any());
    }

    @Test
    void missingAmbiguousAndWrongTypeCommandsAreInconsistent() {
        SagaInstance scoring = saga(SagaStep.SCORING);
        assertEquals(SagaRecoveryClassification.INCONSISTENT,
                SagaRecoveryService.classify(SagaStep.SCORING, List.of()));
        assertEquals(SagaRecoveryClassification.INCONSISTENT,
                SagaRecoveryService.classify(SagaStep.SCORING,
                        List.of(command(scoring.getApplicationId(), "MakeDecision", SagaOutboxStatus.PUBLISHED))));
        var matching = command(scoring.getApplicationId(), "PerformCreditScoring", SagaOutboxStatus.NEW);
        assertEquals(SagaRecoveryClassification.INCONSISTENT,
                SagaRecoveryService.classify(SagaStep.SCORING, List.of(matching, matching)));
        assertEquals(SagaRecoveryClassification.INCONSISTENT,
                SagaRecoveryService.classify(SagaStep.FINISHED, List.of()));
    }

    @Test
    void retryOutboxRowsAreClassifiedByNewestPublicationStatus() {
        UUID applicationId = UUID.randomUUID();
        var original = command(applicationId, "PerformBureauCheck", SagaOutboxStatus.PUBLISHED);
        var retry = command(applicationId, "PerformBureauCheck", SagaOutboxStatus.NEW);
        when(original.getCreatedAt()).thenReturn(Instant.parse("2026-10-12T09:00:00Z"));
        when(retry.getCreatedAt()).thenReturn(Instant.parse("2026-10-12T09:01:00Z"));
        assertEquals(SagaRecoveryClassification.WAITING_FOR_PUBLICATION,
                SagaRecoveryService.classify(SagaStep.BUREAU, List.of(original, retry)));
    }

    @Test
    void scansBoundedPagesAndRepeatedInspectionIsReadOnly() {
        SagaInstance scoring = saga(SagaStep.SCORING);
        SagaInstance bureau = saga(SagaStep.BUREAU);
        SagaInstance decision = saga(SagaStep.DECISION);
        when(sagas.findByStatus(eq(SagaStatus.IN_PROGRESS), any())).thenAnswer(invocation -> {
            PageRequest request = invocation.getArgument(1);
            return request.getPageNumber() == 0
                    ? new SliceImpl<>(List.of(scoring, bureau), request, true)
                    : new SliceImpl<>(List.of(decision), request, false);
        });
        when(outbox.findDiagnosticRows(anyCollection())).thenAnswer(invocation -> {
            List<UUID> ids = List.copyOf(invocation.<java.util.Collection<UUID>>getArgument(0));
            return ids.stream().map(id -> id.equals(scoring.getApplicationId())
                    ? command(id, "PerformCreditScoring", SagaOutboxStatus.NEW)
                    : id.equals(bureau.getApplicationId())
                        ? command(id, "PerformBureauCheck", SagaOutboxStatus.CLAIMED)
                        : command(id, "MakeDecision", SagaOutboxStatus.PUBLISHED)).toList();
        });

        assertEquals(3, recovery.inspect().inspected());
        assertEquals(3, recovery.inspect().inspected());
        verify(sagas, times(4)).findByStatus(eq(SagaStatus.IN_PROGRESS), any());
        verify(sagas, never()).save(any());
        verify(outbox, never()).save(any());
    }

    @Test
    void completedSagaDoesNotEnterRecoveryScan() {
        when(sagas.findByStatus(eq(SagaStatus.IN_PROGRESS), any()))
                .thenReturn(new SliceImpl<>(List.of()));
        assertEquals(0, recovery.inspect().inspected());
        verify(outbox, never()).findDiagnosticRows(anyCollection());
        verify(sagas, never()).findByStatus(eq(SagaStatus.COMPLETED), any());
    }

    @Test
    void scanStopsAtMaximumAndMarksTruncated() {
        SagaInstance a = saga(SagaStep.SCORING), b = saga(SagaStep.SCORING);
        SagaRecoveryService limited = new SagaRecoveryService(sagas, outbox, 2, 1);
        when(sagas.findByStatus(eq(SagaStatus.IN_PROGRESS), any()))
                .thenReturn(new SliceImpl<>(List.of(a, b), PageRequest.of(0, 2), true));
        when(outbox.findDiagnosticRows(anyCollection())).thenReturn(List.of());
        var result = limited.inspect();
        assertEquals(1, result.inspected());
        assertTrue(result.truncated());
    }

    private void givenSinglePage(List<SagaInstance> batch, List<SagaOutboxDiagnosticRow> commands) {
        when(sagas.findByStatus(eq(SagaStatus.IN_PROGRESS), any()))
                .thenReturn(new SliceImpl<>(batch));
        when(outbox.findDiagnosticRows(anyCollection())).thenReturn(commands);
    }

    private static SagaInstance saga(SagaStep step) {
        SagaInstance mock = mock(SagaInstance.class);
        when(mock.getSagaId()).thenReturn(UUID.randomUUID());
        when(mock.getApplicationId()).thenReturn(UUID.randomUUID());
        when(mock.getCurrentStep()).thenReturn(step);
        return mock;
    }

    private static SagaOutboxDiagnosticRow command(UUID applicationId, String type, SagaOutboxStatus status) {
        SagaOutboxDiagnosticRow mock = mock(SagaOutboxDiagnosticRow.class);
        when(mock.getApplicationId()).thenReturn(applicationId);
        when(mock.getCommandType()).thenReturn(type);
        when(mock.getStatus()).thenReturn(status);
        return mock;
    }
}
