<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema;

use function Flow\ETL\DSL\{bool_schema,
    integer_schema,
    schema,
    schema_selective_validator,
    schema_validate,
    string_schema};
use Flow\ETL\{Schema\Metadata, Tests\FlowTestCase};
use Flow\ETL\Schema\Validator\SelectiveValidator;

final class SelectiveValidatorTest extends FlowTestCase
{
    public function test_given_schema_non_nullable_expected_nullable() : void
    {
        self::assertTrue(
            schema_validate(
                expected: schema(integer_schema('id'), string_schema('name'), bool_schema('active', true)),
                given: schema(integer_schema('id'), string_schema('name'), bool_schema('active')),
                validator: schema_selective_validator()
            )
        );
    }

    public function test_given_schema_nullable_expected_non_nullable() : void
    {
        self::assertFalse(
            schema_validate(
                expected: schema(integer_schema('id'), string_schema('name'), bool_schema('active')),
                given: schema(integer_schema('id'), string_schema('name'), bool_schema('active', true)),
                validator: schema_selective_validator()
            )
        );
    }

    public function test_schema_with_a_missing_entry() : void
    {
        self::assertFalse(
            schema_validate(
                schema(integer_schema('id'), string_schema('name')),
                schema(integer_schema('id'), bool_schema('active')),
                schema_selective_validator()
            )
        );
    }

    public function test_schema_with_an_extra_entry() : void
    {
        self::assertTrue(
            schema_validate(
                schema(integer_schema('id'), string_schema('name')),
                schema(integer_schema('id'), string_schema('name'), bool_schema('active', true)),
                schema_selective_validator()
            )
        );
    }

    public function test_schema_with_from_null_metadata() : void
    {
        self::assertTrue(
            (new SelectiveValidator())->isValid(
                schema(integer_schema('id', nullable: true)),
                schema(string_schema('id', nullable: true, metadata: Metadata::with(Metadata::FROM_NULL, true))),
            )
        );

        self::assertFalse(
            (new SelectiveValidator())->isValid(
                schema(integer_schema('id', nullable: true)),
                schema(string_schema('id', nullable: true)),
            )
        );
    }

    public function test_schema_with_multiple_columns_with_from_null_metadata() : void
    {
        self::assertTrue(
            (new SelectiveValidator())->isValid(
                expected: schema(
                    integer_schema('id', nullable: true),
                    string_schema('name')
                ),
                given: schema(
                    string_schema('id', nullable: true, metadata: Metadata::with(Metadata::FROM_NULL, true)),
                    string_schema('name')
                )
            )
        );

        self::assertFalse(
            (new SelectiveValidator())->isValid(
                expected: schema(
                    integer_schema('id', nullable: true),
                    string_schema('name', nullable: true)
                ),
                given: schema(
                    string_schema('id', nullable: true),
                    string_schema('name')
                )
            )
        );

        self::assertFalse(
            (new SelectiveValidator())->isValid(
                expected: schema(
                    integer_schema('id', nullable: true),
                    string_schema('name')
                ),
                given: schema(
                    string_schema('id', nullable: true),
                    string_schema('name')
                )
            )
        );
    }

    public function test_schema_with_single_invalid_column() : void
    {
        self::assertFalse(
            (new SelectiveValidator())->isValid(
                schema(integer_schema('id'), bool_schema('name'), bool_schema('active')),
                schema(integer_schema('id'), string_schema('name'), bool_schema('active', true))
            )
        );
    }

    public function test_with_from_null_metadata_but_non_string_type() : void
    {
        self::assertFalse(
            (new SelectiveValidator())->isValid(
                expected: schema(integer_schema('id', nullable: true)),
                given: schema(bool_schema('id', nullable: true, metadata: Metadata::with(Metadata::FROM_NULL, true))),
            )
        );
    }
}
