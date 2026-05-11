<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Function\ExecutionMode;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\map_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\string_entry;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;

final class ArrayValuesTest extends FlowTestCase
{
    public function test_array_values(): void
    {
        static::assertSame(
            [1, 2],
            ref('map')
                ->arrayValues()
                ->eval(
                    row(map_entry('map', ['a' => 1, 'b' => 2], type_map(type_string(), type_integer()))),
                    flow_context(),
                ),
        );
    }

    public function test_array_values_in_strict_mode(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('ArrayValues function requires non-null array');

        $context = flow_context(config());
        $context->functions()->setMode(ExecutionMode::STRICT);

        ref('map')->arrayValues()->eval(row(string_entry('map', 'test')), $context);
    }

    public function test_array_values_on_non_array(): void
    {
        static::assertNull(ref('map')->arrayValues()->eval(row(string_entry('map', 'test')), flow_context()));
    }
}
