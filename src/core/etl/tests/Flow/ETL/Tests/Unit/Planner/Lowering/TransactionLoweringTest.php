<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planner\Lowering;

use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Plan\Node\Transaction;
use Flow\ETL\Plan\Node\Write;
use Flow\ETL\Planner\Lowering\TransactionLowering;
use Flow\ETL\Tests\Double\RecordingTransaction;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

use function Flow\ETL\DSL\to_memory;

final class TransactionLoweringTest extends FlowTestCase
{
    public function test_handles_transaction(): void
    {
        static::assertSame(Transaction::class, (new TransactionLowering())->handles());
    }

    public function test_a_transaction_owns_no_step(): void
    {
        static::assertSame(
            [],
            (new TransactionLowering())->steps(
                new Transaction(
                    new RecordingTransaction(),
                    new Write(NodeMother::read(), to_memory(new ArrayMemory())),
                ),
                NodeMother::context(),
                [],
            ),
        );
    }
}
