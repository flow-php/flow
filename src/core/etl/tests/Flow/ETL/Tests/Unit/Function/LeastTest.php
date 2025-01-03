<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{int_entry, least, ref, row};
use Flow\ETL\Tests\FlowTestCase;

final class LeastTest extends FlowTestCase
{
    public function test_greatest_with_non_comparable_values() : void
    {
        $lest = least(
            null,
            20,
            ref('int'),
            new \DateTimeImmutable('now'),
        );

        $this->expectExceptionMessage("Can't compare '(datetime < integer)' due to data type mismatch.");

        $lest->eval(row(int_entry('int', 55)));
    }

    public function test_least_value() : void
    {
        $lest = least(
            10,
            20,
            ref('int'),
            40,
        );

        self::assertSame(
            10,
            $lest->eval(row(int_entry('int', 55)))
        );
    }

    public function test_least_with_null() : void
    {
        $ltest = least(
            null,
            20,
            ref('int'),
            1257
        );

        self::assertNull(
            $ltest->eval(row(int_entry('int', 4)))
        );
    }
}
