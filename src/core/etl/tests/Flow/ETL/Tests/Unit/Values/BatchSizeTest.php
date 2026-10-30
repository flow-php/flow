<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Values;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\BatchSize;
use PHPUnit\Framework\Attributes\TestWith;

final class BatchSizeTest extends FlowTestCase
{
    public function test_the_default_size_is_2048(): void
    {
        static::assertSame(2048, (new BatchSize())->size);
    }

    public function test_one_row_is_the_smallest_batch(): void
    {
        static::assertSame(1, (new BatchSize(1))->size);
    }

    #[TestWith([0])]
    #[TestWith([-1])]
    public function test_a_size_below_one_is_refused(int $size): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Batch size must be greater than 0, got ' . $size);

        new BatchSize($size);
    }
}
