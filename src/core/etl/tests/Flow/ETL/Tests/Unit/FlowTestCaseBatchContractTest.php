<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit;

use Flow\ETL\Tests\Double\CountingExtractor;
use Flow\ETL\Tests\Double\OneRowBatchesExtractor;
use Flow\ETL\Tests\Double\StopIgnoringExtractor;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\RowsMother;
use PHPUnit\Framework\ExpectationFailedException;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;

final class FlowTestCaseBatchContractTest extends FlowTestCase
{
    public function test_contract_fails_when_stop_is_swallowed(): void
    {
        $this->expectException(ExpectationFailedException::class);

        self::assertExtractorHonoursBatchContract(
            static fn(): StopIgnoringExtractor => new StopIgnoringExtractor(RowsMother::sequentialIds(5)),
            RowsMother::sequentialIds(5),
        );
    }

    public function test_contract_fails_when_the_source_never_batches(): void
    {
        $this->expectException(ExpectationFailedException::class);

        self::assertExtractorHonoursBatchContract(
            static fn(): OneRowBatchesExtractor => new OneRowBatchesExtractor(RowsMother::sequentialIds(5)),
            RowsMother::sequentialIds(5),
        );
    }

    public function test_contract_holds_for_a_varying_batch_source(): void
    {
        $batches = [
            RowsMother::sequentialIds(1),
            RowsMother::sequentialIds(1000),
            RowsMother::sequentialIds(0),
            RowsMother::sequentialIds(7),
            RowsMother::sequentialIds(999),
        ];

        $expected = RowsMother::sequentialIds(0);

        foreach ($batches as $batch) {
            $expected = $expected->merge($batch);
        }

        self::assertExtractorHonoursBatchContract(
            static fn(): CountingExtractor => new CountingExtractor(schema(int_schema('id')), ...$batches),
            $expected,
        );
    }
}
