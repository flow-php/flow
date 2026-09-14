<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Pipeline;

use Flow\ETL\Pipeline\TransactionRollback;
use Flow\ETL\Tests\Context\MemoryTelemetryContext;
use Flow\ETL\Tests\Double\RecordingTransaction;
use Flow\ETL\Tests\FlowTestCase;
use RuntimeException;

final class TransactionRollbackTest extends FlowTestCase
{
    public function test_rollback_is_delegated_with_its_cause(): void
    {
        $telemetry = new MemoryTelemetryContext();
        $cause = new RuntimeException('boom');
        $transaction = new RecordingTransaction();

        (new TransactionRollback($transaction))->rollback($cause, $telemetry->flowContext);

        static::assertSame(['rollback'], $transaction->log);
        static::assertSame([$cause], $transaction->rolledBackFor);
        static::assertCount(0, $telemetry->logs->entriesContaining('Transaction failed to roll back.'));
    }

    public function test_a_rollback_failure_is_logged_not_thrown(): void
    {
        $telemetry = new MemoryTelemetryContext();
        $transaction = new RecordingTransaction(rollbackFailure: new RuntimeException('rollback failed'));

        (new TransactionRollback($transaction))->rollback(new RuntimeException('boom'), $telemetry->flowContext);

        static::assertSame(['rollback'], $transaction->log);
        static::assertCount(1, $telemetry->logs->entriesContaining('Transaction failed to roll back.'));
    }
}
