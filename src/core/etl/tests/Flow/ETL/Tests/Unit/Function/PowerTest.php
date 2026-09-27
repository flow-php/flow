<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_row;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class PowerTest extends FlowTestCase
{
    public function test_power_non_numeric_values(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected type "integer", got "string".');

        ref('int')
            ->power(lit('non numeric'))
            ->eval(array_to_row(['int' => 10], schema(int_schema('int'))), flow_context());
        ref('str')->power(lit(2))->eval(array_to_row(['str' => 'abc'], schema(str_schema('str'))), flow_context());
    }

    public function test_power_two_numeric_values(): void
    {
        static::assertSame(100, ref('int')
            ->power(lit(2))
            ->eval(array_to_row(['int' => 10], schema(int_schema('int'))), flow_context()));
    }
}
