<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\Double\CountingExtractor;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\TestWith;

use function Flow\ETL\DSL\schema;

final class BatchesTest extends FlowTestCase
{
    public function test_batch_size_returns_what_was_set(): void
    {
        static::assertSame(
            10,
            (new CountingExtractor(schema()))
                ->withBatchSize(10)
                ->batchSize(),
        );
    }

    public function test_default_batch_size_is_one_hundred(): void
    {
        static::assertSame(100, (new CountingExtractor(schema()))->batchSize());
    }

    #[TestWith([0])]
    #[TestWith([-1])]
    public function test_throws_when_batch_size_is_not_positive(int $batchSize): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Batch size must be greater than 0, got ' . $batchSize);

        (new CountingExtractor(schema()))->withBatchSize($batchSize);
    }

    public function test_with_batch_size_returns_the_same_instance(): void
    {
        $extractor = new CountingExtractor(schema());

        static::assertSame($extractor, $extractor->withBatchSize(10));
    }
}
