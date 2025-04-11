<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Schema;

use function Flow\ETL\DSL\{bool_schema, integer_schema, list_schema, schema, string_schema};
use function Flow\ETL\DSL\{type_list, type_string};
use Flow\ETL\{Schema\Metadata, Tests\FlowTestCase};
use Flow\ETL\Schema\Validator\StrictValidator;

final class StrictValidatorTest extends FlowTestCase
{
    public function test_given_schema_non_nullable_expected_nullable() : void
    {
        self::assertTrue(
            (new StrictValidator())->isValid(
                given: schema(integer_schema('id'), string_schema('name'), bool_schema('active')),
                expected: schema(integer_schema('id'), string_schema('name'), bool_schema('active', true))
            )
        );
    }

    public function test_given_schema_nullable_expected_non_nullable() : void
    {
        self::assertFalse(
            (new StrictValidator())->isValid(
                given: schema(integer_schema('id'), string_schema('name'), bool_schema('active', true)),
                expected: schema(integer_schema('id'), string_schema('name'), bool_schema('active'))
            )
        );
    }

    public function test_rows_with_a_missing_entry() : void
    {
        self::assertFalse(
            (new StrictValidator())->isValid(
                given: schema(integer_schema('id'), string_schema('name')),
                expected: schema(integer_schema('id'), string_schema('name'), bool_schema('active', true))
            )
        );
    }

    public function test_rows_with_an_extra_entry() : void
    {
        self::assertFalse(
            (new StrictValidator())->isValid(
                given: schema(integer_schema('id'), string_schema('name'), bool_schema('active'), list_schema('tags', type_list(type_string()))),
                expected: schema(integer_schema('id'), string_schema('name'), bool_schema('active')),
            )
        );
    }

    public function test_rows_with_from_null_metadata() : void
    {
        self::assertTrue(
            (new StrictValidator())->isValid(
                given: schema(string_schema('id', nullable: true, metadata: Metadata::with(Metadata::FROM_NULL, true))),
                expected: schema(integer_schema('id', nullable: true)),
            )
        );

        self::assertFalse(
            (new StrictValidator())->isValid(
                given: schema(integer_schema('id', nullable: true)),
                expected: schema(string_schema('id', nullable: true))
            )
        );
    }

    public function test_rows_with_multiple_columns_with_from_null_metadata() : void
    {

        self::assertFalse(
            (new StrictValidator())->isValid(
                given: schema(string_schema('id', nullable: true), string_schema('name')),
                expected: schema(integer_schema('id', nullable: true), string_schema('name'))
            )
        );

        self::assertTrue(
            (new StrictValidator())->isValid(
                given: schema(string_schema('id', nullable: true, metadata: Metadata::with(Metadata::FROM_NULL, true)), string_schema('name')),
                expected: schema(integer_schema('id', nullable: true), string_schema('name'))
            )
        );
    }

    public function test_with_from_null_metadata_but_non_string_type() : void
    {
        self::assertFalse(
            (new StrictValidator())->isValid(
                given: schema(bool_schema('id', nullable: true, metadata: Metadata::with(Metadata::FROM_NULL, true))),
                expected: schema(integer_schema('id', nullable: true)),
            )
        );
    }
}
