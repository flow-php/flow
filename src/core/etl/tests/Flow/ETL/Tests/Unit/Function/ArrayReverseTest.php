<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\{int_entry, json_entry, ref};
use Flow\ETL\Tests\FlowTestCase;

final class ArrayReverseTest extends FlowTestCase
{
    public function test_array_reverse_array_entry() : void
    {
        self::assertSame(
            [5, 3, 10, 4],
            ref('a')->arrayReverse()
                ->eval(
                    row(json_entry('a', [4, 10, 3, 5])),
                )
        );
    }

    public function test_array_reverse_non_array_entry() : void
    {
        self::assertNull(
            ref('a')->arrayReverse()
                ->eval(
                    row(int_entry('a', 123)),
                )
        );
    }
}
