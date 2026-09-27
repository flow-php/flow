<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Function\Exists;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_row;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class ExistsTest extends FlowTestCase
{
    public function test_a_throwing_operand_means_the_reference_does_not_exist(): void
    {
        static::assertFalse((new Exists(ref('value')->upper()))->eval(array_to_row([
            'value' => 1,
        ], schema(int_schema('value'))), flow_context()));
    }

    public function test_if_reference_exists(): void
    {
        static::assertTrue(
            ref('value')
                ->exists()
                ->eval(array_to_row(['value' => 'test'], schema(str_schema('value'))), flow_context()),
        );
    }

    public function test_that_lit_function_exists(): void
    {
        static::assertTrue((new Exists(lit('val')))->eval(array_to_row([], schema()), flow_context()));
    }

    public function test_that_null_reference_to_null_entry_exists(): void
    {
        static::assertTrue(
            ref('value')
                ->exists()
                ->eval(array_to_row(['value' => null], schema(str_schema('value', nullable: true))), flow_context()),
        );
    }

    public function test_that_reference_does_not_exists(): void
    {
        static::assertFalse(ref('value')->exists()->eval(array_to_row([], schema()), flow_context()));
    }
}
