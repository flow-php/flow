<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema;

use function Flow\ETL\DSL\{bool_schema, integer_schema, list_schema, schema, string_schema};
use function Flow\ETL\DSL\{schema_strict_validator, schema_validate};
use function Flow\Types\DSL\{type_list, type_string};
use Flow\ETL\{Schema\Metadata, Tests\FlowTestCase};

final class StrictValidatorTest extends FlowTestCase
{
    public function test_given_schema_non_nullable_expected_nullable() : void
    {
        self::assertTrue(
            schema_validate(
                expected: schema(integer_schema('id'), string_schema('name'), bool_schema('active', true)),
                given: schema(integer_schema('id'), string_schema('name'), bool_schema('active')),
                validator: schema_strict_validator()
            )
        );
    }

    public function test_given_schema_nullable_expected_non_nullable() : void
    {
        self::assertFalse(
            schema_validate(
                expected: schema(integer_schema('id'), string_schema('name'), bool_schema('active')),
                given: schema(integer_schema('id'), string_schema('name'), bool_schema('active', true)),
                validator: schema_strict_validator()
            )
        );
    }

    public function test_rows_with_a_missing_entry() : void
    {
        self::assertFalse(
            schema_validate(
                expected: schema(integer_schema('id'), string_schema('name'), bool_schema('active', true)),
                given: schema(integer_schema('id'), string_schema('name')),
                validator: schema_strict_validator()
            )
        );
    }

    public function test_rows_with_an_extra_entry() : void
    {
        self::assertFalse(
            schema_validate(
                expected: schema(integer_schema('id'), string_schema('name'), bool_schema('active')),
                given: schema(
                    integer_schema('id'),
                    string_schema('name'),
                    bool_schema('active'),
                    list_schema('tags', type_list(type_string()))
                ),
                validator: schema_strict_validator()
            ),
        );
    }

    public function test_rows_with_from_null_metadata() : void
    {
        self::assertTrue(
            schema_validate(
                expected: schema(integer_schema('id', nullable: true)),
                given: schema(string_schema('id', nullable: true, metadata: Metadata::with(Metadata::FROM_NULL, true))),
                validator: schema_strict_validator()
            )
        );

        self::assertFalse(
            schema_validate(
                expected: schema(string_schema('id', nullable: true)),
                given: schema(integer_schema('id', nullable: true)),
                validator: schema_strict_validator()
            )
        );
    }

    public function test_rows_with_multiple_columns_with_from_null_metadata() : void
    {

        self::assertFalse(
            schema_validate(
                expected: schema(integer_schema('id', nullable: true), string_schema('name')),
                given: schema(string_schema('id', nullable: true), string_schema('name')),
                validator: schema_strict_validator()
            )
        );

        self::assertTrue(
            schema_validate(
                expected: schema(integer_schema('id', nullable: true), string_schema('name')),
                given: schema(
                    string_schema('id', nullable: true, metadata: Metadata::with(Metadata::FROM_NULL, true)),
                    string_schema('name')
                ),
                validator: schema_strict_validator()
            )
        );
    }

    public function test_with_from_null_metadata_but_non_string_type() : void
    {
        self::assertFalse(
            schema_validate(
                expected: schema(integer_schema('id', nullable: true)),
                given: schema(bool_schema('id', nullable: true, metadata: Metadata::with(Metadata::FROM_NULL, true))),
                validator: schema_strict_validator()
            )
        );
    }
}
