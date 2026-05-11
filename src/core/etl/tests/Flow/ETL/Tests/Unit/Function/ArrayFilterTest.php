<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Function\ExecutionMode;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\list_entry;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\string_entry;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;

final class ArrayFilterTest extends FlowTestCase
{
    public function test_array_filter(): void
    {
        static::assertSame(
            [1],
            ref('list')
                ->arrayFilter(lit(2))
                ->eval(row(list_entry('list', [1, 2], type_list(type_integer()))), flow_context()),
        );
    }

    public function test_array_filter_by_entry_reference(): void
    {
        static::assertSame(
            [1],
            ref('list')
                ->arrayFilter(ref('int'))
                ->eval(row(list_entry('list', [1, 2], type_list(type_integer())), int_entry('int', 2)), flow_context()),
        );
    }

    public function test_array_filter_in_strict_mode(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('ArrayFilter function requires non-null array');

        $context = flow_context(config());
        $context->functions()->setMode(ExecutionMode::STRICT);

        ref('map')->arrayFilter(lit(1))->eval(row(string_entry('map', 'test')), $context);
    }

    public function test_array_filter_not_existing_value(): void
    {
        static::assertSame(
            [1, 2],
            ref('list')
                ->arrayFilter(lit(5))
                ->eval(row(list_entry('list', [1, 2], type_list(type_integer()))), flow_context()),
        );
    }

    public function test_array_filter_on_non_array(): void
    {
        static::assertNull(ref('map')->arrayFilter(lit(1))->eval(row(string_entry('map', 'test')), flow_context()));
    }
}
