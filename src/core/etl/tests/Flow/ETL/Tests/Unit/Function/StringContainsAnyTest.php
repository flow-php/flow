<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_row;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;

final class StringContainsAnyTest extends FlowTestCase
{
    public function test_contains_any_empty_needles_array(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('StringContainsAny function requires a non-empty needles array');

        ref('str')
            ->stringContainsAny([])
            ->eval(array_to_row(['str' => 'hello world'], schema(str_schema('str'))), flow_context());
    }

    public function test_contains_any_multiple_needles_one_found(): void
    {
        static::assertTrue(
            ref('str')
                ->stringContainsAny(['foo', 'world', 'bar'])
                ->eval(array_to_row(['str' => 'hello world'], schema(str_schema('str'))), flow_context()),
        );
    }

    public function test_contains_any_no_needles_found(): void
    {
        static::assertFalse(
            ref('str')
                ->stringContainsAny(['foo', 'bar', 'baz'])
                ->eval(array_to_row(['str' => 'hello world'], schema(str_schema('str'))), flow_context()),
        );
    }

    public function test_contains_any_null_needles(): void
    {
        static::assertNull(
            ref('str')
                ->stringContainsAny(ref('needles'))
                ->eval(
                    array_to_row(
                        ['str' => 'hello world', 'needles' => null],
                        schema(str_schema('str'), str_schema('needles', nullable: true)),
                    ),
                    flow_context(),
                ),
        );
    }

    public function test_contains_any_null_string(): void
    {
        static::assertNull(
            ref('str')
                ->stringContainsAny(['hello', 'world'])
                ->eval(array_to_row(['str' => null], schema(str_schema('str', nullable: true))), flow_context()),
        );
    }

    public function test_contains_any_null_string_in_strict_mode(): void
    {
        $context = flow_context(config());
        static::assertNull(
            ref('str')
                ->stringContainsAny(['hello', 'world'])
                ->eval(array_to_row(['str' => null], schema(str_schema('str', nullable: true))), $context),
        );
    }

    public function test_contains_any_with_scalar_function_parameter(): void
    {
        static::assertTrue(
            ref('str')
                ->stringContainsAny(ref('needles'))
                ->eval(
                    array_to_row(
                        ['str' => 'hello world', 'needles' => ['world', 'foo']],
                        schema(str_schema('str'), list_schema('needles', type_list(type_string()))),
                    ),
                    flow_context(),
                ),
        );
    }
}
