<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Function\ArrayExpand\ArrayExpand;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_expand;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\json_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;

final class ArrayExpandTest extends FlowTestCase
{
    public function test_expand_both(): void
    {
        $row = row(json_entry('array', ['a' => 1, 'b' => 2, 'c' => 3]));

        static::assertSame(
            [
                ['a' => 1],
                ['b' => 2],
                ['c' => 3],
            ],
            array_expand(ref('array'), ArrayExpand::BOTH)->eval($row, flow_context()),
        );
    }

    public function test_expand_keys(): void
    {
        $row = row(json_entry('array', ['a' => 1, 'b' => 2, 'c' => 3]));

        static::assertSame(['a', 'b', 'c'], array_expand(ref('array'), ArrayExpand::KEYS)->eval($row, flow_context()));
    }

    public function test_expand_values(): void
    {
        $row = row(json_entry('array', ['a' => 1, 'b' => 2, 'c' => 3]));

        static::assertSame(['a' => 1, 'b' => 2, 'c' => 3], array_expand(ref('array'))->eval($row, flow_context()));
    }

    public function test_for_not_array_entry(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected type "array<mixed>", got "integer".');

        array_expand(ref('integer_entry'))->eval(row(int_entry('integer_entry', 1)), flow_context());
    }

    public function test_for_not_array_entry_in_strict_mode(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected type "array<mixed>", got "integer".');

        $context = flow_context(config());
        array_expand(ref('integer_entry'))->eval(row(int_entry('integer_entry', 1)), $context);
    }
}
