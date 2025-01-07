<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\{concat, json_entry, lit, ref};
use Flow\ETL\Tests\FlowTestCase;

final class ConcatTest extends FlowTestCase
{
    public function test_concat_arrays() : void
    {
        self::assertSame(
            '["a"]["b","c"]',
            concat(ref('array_1'), ref('array_2'))->eval(row(json_entry('array_1', ['a']), json_entry('array_2', ['b', 'c']))),
        );
    }

    public function test_concat_different_types_of_values() : void
    {
        self::assertSame(
            '1abc["a","b"]',
            concat(lit(1), lit('a'), lit('b'), lit('c'), lit(['a', 'b']))->eval(row()),
        );
    }

    public function test_concat_string_values() : void
    {
        self::assertSame(
            'abc',
            concat(lit('a'), lit('b'), lit('c'))->eval(row()),
        );
    }
}
