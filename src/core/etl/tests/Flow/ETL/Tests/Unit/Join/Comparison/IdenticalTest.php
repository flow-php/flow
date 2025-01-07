<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Join\Comparison;

use function Flow\ETL\DSL\{int_entry, row};
use Flow\ETL\Join\Comparison\Identical;
use Flow\ETL\Tests\FlowTestCase;

final class IdenticalTest extends FlowTestCase
{
    public function test_failure() : void
    {
        self::assertFalse(
            (new Identical('id', 'id'))->compare(
                row(int_entry('id', 1)),
                row(int_entry('id', 2)),
            )
        );
    }

    public function test_success() : void
    {
        self::assertTrue(
            (new Identical('id', 'id'))->compare(
                row(int_entry('id', 1)),
                row(int_entry('id', 1)),
            )
        );
    }
}
