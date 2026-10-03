<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column\Layout;

use Flow\ETL\Column\Layout\NullCount;
use PHPUnit\Framework\TestCase;

final class NullCountTest extends TestCase
{
    public function test_counts_nulls(): void
    {
        static::assertSame(2, (new NullCount())->of([null, 0, '', null, false]));
        static::assertSame(0, (new NullCount())->of([]));
    }
}
