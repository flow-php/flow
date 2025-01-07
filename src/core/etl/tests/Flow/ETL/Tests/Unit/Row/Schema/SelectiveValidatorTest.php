<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Schema;

use function Flow\ETL\DSL\{bool_entry, int_entry, str_entry};
use function Flow\ETL\DSL\{bool_schema, integer_schema, schema, string_schema};
use function Flow\ETL\DSL\{row, rows};
use Flow\ETL\Row\Schema\SelectiveValidator;
use Flow\ETL\{Tests\FlowTestCase};

final class SelectiveValidatorTest extends FlowTestCase
{
    public function test_rows_with_a_missing_entry() : void
    {
        $schema = schema(integer_schema('id'), string_schema('name'));

        self::assertFalse(
            (new SelectiveValidator())->isValid(
                rows(row(int_entry('id', 1), bool_entry('active', true))),
                $schema
            )
        );
    }

    public function test_rows_with_an_extra_entry() : void
    {
        $schema = schema(integer_schema('id'), string_schema('name'));

        self::assertTrue(
            (new SelectiveValidator())->isValid(
                rows(row(int_entry('id', 1), str_entry('name', 'test'), bool_entry('active', true))),
                $schema
            )
        );
    }

    public function test_rows_with_single_invalid_entry() : void
    {
        $schema = schema(integer_schema('id'), bool_schema('name'), bool_schema('active'));

        self::assertFalse(
            (new SelectiveValidator())->isValid(
                rows(row(int_entry('id', 1), str_entry('name', 'test'), bool_entry('active', true))),
                $schema
            )
        );
    }

    public function test_rows_with_single_invalid_row() : void
    {
        $schema = schema(string_schema('name'), bool_schema('active'));

        self::assertFalse(
            (new SelectiveValidator())->isValid(
                rows(row(int_entry('id', 1), str_entry('name', 'test'), bool_entry('active', true)), row(int_entry('id', 1), bool_entry('active', true))),
                $schema
            )
        );
    }
}
