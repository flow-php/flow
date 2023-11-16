<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\concat;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use Flow\ETL\DSL\Entry;
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class ConcatTest extends TestCase
{
    public function test_concat_arrays() : void
    {
        $this->assertSame(
            '["a"]["b","c"]',
            concat(ref('array_1'), ref('array_2'))->eval(Row::create(Entry::array('array_1', ['a']), Entry::array('array_2', ['b', 'c']))),
        );
    }

    public function test_concat_different_types_of_values() : void
    {
        $this->assertSame(
            '1abc["a","b"]',
            concat(lit(1), lit('a'), lit('b'), lit('c'), lit(['a', 'b']))->eval(Row::create()),
        );
    }

    public function test_concat_string_values() : void
    {
        $this->assertSame(
            'abc',
            concat(lit('a'), lit('b'), lit('c'))->eval(Row::create()),
        );
    }
}
