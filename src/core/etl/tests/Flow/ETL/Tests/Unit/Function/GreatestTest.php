<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{greatest, int_entry, ref, row};
use Flow\ETL\Tests\FlowTestCase;

final class GreatestTest extends FlowTestCase
{
    public function test_greatest_value() : void
    {
        $greatest = greatest(
            10,
            20,
            ref('int'),
            40
        );

        self::assertSame(
            55,
            $greatest->eval(row(int_entry('int', 55)))
        );
    }

    public function test_greatest_with_non_comparable_values() : void
    {
        $greatest = greatest(
            null,
            20,
            ref('int'),
            new \DateTimeImmutable('now')
        );

        $this->expectExceptionMessage("Can't compare '(datetime > integer)' due to data type mismatch.");

        $greatest->eval(row(int_entry('int', 55)));
    }

    public function test_greatest_with_null() : void
    {
        $greatest = greatest(
            null,
            20,
            ref('int'),
            1257
        );

        self::assertSame(
            1257,
            $greatest->eval(row(int_entry('int', 55)))
        );
    }
}
