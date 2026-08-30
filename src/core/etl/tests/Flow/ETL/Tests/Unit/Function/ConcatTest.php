<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\concat;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;

final class ConcatTest extends FlowTestCase
{
    public function test_concat_arrays(): void
    {
        static::assertSame('["a"]["b","c"]', concat(ref('array_1'), ref('array_2'))->eval(row([
            'array_1' => ['a'],
            'array_2' => ['b', 'c'],
        ]), flow_context()));
    }

    public function test_concat_different_types_of_values(): void
    {
        static::assertSame('1abc["a","b"]', concat(lit(1), lit('a'), lit('b'), lit('c'), lit(['a', 'b']))->eval(
            row([]),
            flow_context(),
        ));
    }

    public function test_concat_string_values(): void
    {
        static::assertSame('abc', concat(lit('a'), lit('b'), lit('c'))->eval(row([]), flow_context()));
    }
}
