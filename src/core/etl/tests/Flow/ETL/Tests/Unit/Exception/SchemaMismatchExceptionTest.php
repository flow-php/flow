<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Exception;

use Flow\ETL\Exception\ColumnMismatchException;
use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\str_schema;

final class SchemaMismatchExceptionTest extends FlowTestCase
{
    public function test_it_keeps_the_row_violation_as_the_cause(): void
    {
        $cause = ColumnMismatchException::unexpectedColumn('extra');

        static::assertSame($cause, (new SchemaMismatchException(2, $cause))->getPrevious());
    }

    public function test_it_places_a_column_violation_at_its_row(): void
    {
        static::assertSame(
            'Rows do not match their schema: column "extra" (row 2) is not declared by the schema',
            (new SchemaMismatchException(2, ColumnMismatchException::unexpectedColumn('extra')))->getMessage(),
        );
    }

    public function test_it_places_a_value_violation_at_its_row(): void
    {
        static::assertSame(
            'Rows do not match their schema: column "code" (row 1): could not convert 1000 (integer) to string',
            (new SchemaMismatchException(1, ColumnMismatchException::valueDoesNotMatch(
                str_schema('code'),
                1000,
            )))->getMessage(),
        );
    }
}
