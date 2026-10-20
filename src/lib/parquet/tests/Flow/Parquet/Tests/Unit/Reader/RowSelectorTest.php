<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Reader;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Reader\RowSelector;
use PHPUnit\Framework\TestCase;

final class RowSelectorTest extends TestCase
{
    public function test_a_negative_row_count_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Row selector row count must be 0 or more, got -1');

        new RowSelector(-1, false);
    }
}
