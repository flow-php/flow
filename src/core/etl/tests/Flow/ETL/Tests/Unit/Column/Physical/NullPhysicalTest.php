<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column\Physical;

use Flow\ETL\Column\Physical\NullPhysical;
use PHPUnit\Framework\TestCase;

final class NullPhysicalTest extends TestCase
{
    public function test_everything_is_null(): void
    {
        $physical = new NullPhysical();

        static::assertNull($physical->toPhysical('a'));
        static::assertNull($physical->fromPhysical('a'));
        static::assertSame([null, null], $physical->fromPhysicalAll([1, null]));
        static::assertSame([], $physical->fromPhysicalAll([]));
    }
}
