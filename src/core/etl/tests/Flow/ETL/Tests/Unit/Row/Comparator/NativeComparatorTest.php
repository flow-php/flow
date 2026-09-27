<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Comparator;

use Flow\ETL\Row\Comparator\NativeComparator;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_row;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class NativeComparatorTest extends FlowTestCase
{
    public function test_row_comparison(): void
    {
        static::assertTrue((new NativeComparator())->equals(
            array_to_row(['test' => 'test'], schema(str_schema('test'))),
            array_to_row(['test' => 'test'], schema(str_schema('test'))),
            schema(str_schema('test')),
        ));
    }

    public function test_row_comparison_for_different_rows(): void
    {
        static::assertFalse((new NativeComparator())->equals(
            array_to_row(['test' => 'test'], schema(str_schema('test'))),
            array_to_row(['test' => 'other'], schema(str_schema('test'))),
            schema(str_schema('test')),
        ));
    }

    public function test_row_comparison_for_rows_with_different_columns(): void
    {
        static::assertFalse((new NativeComparator())->equals(
            array_to_row(['test' => 'test'], schema(str_schema('test'))),
            array_to_row(['other' => 'test'], schema(str_schema('other'))),
            schema(str_schema('test')),
        ));
    }
}
