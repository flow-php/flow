<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column\Physical;

use Flow\ETL\Column\Physical\IdentityPhysical;
use PHPUnit\Framework\TestCase;

final class IdentityPhysicalTest extends TestCase
{
    public function test_keeps_values(): void
    {
        $physical = new IdentityPhysical();

        static::assertSame('a', $physical->toPhysical('a'));
        static::assertSame(1.5, $physical->fromPhysical(1.5));
        static::assertSame([1, null, true], $physical->fromPhysicalAll([1, null, true]));
    }
}
