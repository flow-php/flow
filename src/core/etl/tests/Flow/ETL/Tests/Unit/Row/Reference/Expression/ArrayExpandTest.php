<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Reference\Expression;

use function Flow\ETL\DSL\array_expand;
use function Flow\ETL\DSL\ref;
use Flow\ETL\DSL\Entry;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression\ArrayExpand\ArrayExpand;
use PHPUnit\Framework\TestCase;

final class ArrayExpandTest extends TestCase
{
    public function test_expand_both() : void
    {
        $row = Row::create(
            Entry::array('array', ['a' => 1, 'b' => 2, 'c' => 3]),
        );

        $this->assertSame(
            [
                ['a' => 1],
                ['b' => 2],
                ['c' => 3],
            ],
            array_expand(ref('array'), ArrayExpand::BOTH)->eval($row)
        );
    }

    public function test_expand_keys() : void
    {
        $row = Row::create(
            Entry::array('array', ['a' => 1, 'b' => 2, 'c' => 3]),
        );

        $this->assertSame(
            ['a', 'b', 'c'],
            array_expand(ref('array'), ArrayExpand::KEYS)->eval($row)
        );
    }

    public function test_expand_values() : void
    {
        $row = Row::create(
            Entry::array('array', ['a' => 1, 'b' => 2, 'c' => 3]),
        );

        $this->assertSame(
            ['a' => 1, 'b' => 2, 'c' => 3],
            array_expand(ref('array'))->eval($row)
        );
    }

    public function test_for_not_array_entry() : void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Flow\ETL\Row\EntryReference is not an array, got: integer');

        array_expand(ref('integer_entry'))->eval(Row::create(Entry::int('integer_entry', 1)));
    }
}
