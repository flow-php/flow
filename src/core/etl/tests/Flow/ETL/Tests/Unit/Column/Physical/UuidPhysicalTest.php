<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column\Physical;

use Flow\ETL\Column\Physical\UuidPhysical;
use Flow\Types\Value\Uuid;
use PHPUnit\Framework\TestCase;

use function bin2hex;
use function Flow\Types\DSL\type_string;

final class UuidPhysicalTest extends TestCase
{
    public function test_sixteen_raw_bytes(): void
    {
        $physical = new UuidPhysical();
        $bytes = type_string()->assert($physical->toPhysical(new Uuid('6c2f1d4e-8b3a-4c5d-9e6f-0a1b2c3d4e5f')));

        static::assertIsString($bytes);
        static::assertSame('6c2f1d4e8b3a4c5d9e6f0a1b2c3d4e5f', bin2hex($bytes));
        static::assertEquals(new Uuid('6c2f1d4e-8b3a-4c5d-9e6f-0a1b2c3d4e5f'), $physical->fromPhysical($bytes));
        static::assertEquals(
            [new Uuid('6c2f1d4e-8b3a-4c5d-9e6f-0a1b2c3d4e5f'), null],
            $physical->fromPhysicalAll([
                $bytes,
                null,
            ]),
        );
    }
}
