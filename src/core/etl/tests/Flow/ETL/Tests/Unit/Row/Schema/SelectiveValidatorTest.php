<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Schema;

use function Flow\ETL\DSL\{bool_schema, integer_schema, schema, string_schema};
use Flow\ETL\{Row\Schema\Metadata, Tests\FlowTestCase};
use Flow\ETL\Row\Schema\SelectiveValidator;

final class SelectiveValidatorTest extends FlowTestCase
{
    public function test_given_schema_non_nullable_expected_nullable() : void
    {
        self::assertTrue(
            (new SelectiveValidator())->isValid(
                given: schema(integer_schema('id'), string_schema('name'), bool_schema('active')),
                expected: schema(integer_schema('id'), string_schema('name'), bool_schema('active', true))
            )
        );
    }

    public function test_given_schema_nullable_expected_non_nullable() : void
    {
        self::assertFalse(
            (new SelectiveValidator())->isValid(
                given: schema(integer_schema('id'), string_schema('name'), bool_schema('active', true)),
                expected: schema(integer_schema('id'), string_schema('name'), bool_schema('active'))
            )
        );
    }

    public function test_schema_with_a_missing_entry() : void
    {
        self::assertFalse(
            (new SelectiveValidator())->isValid(
                schema(integer_schema('id'), bool_schema('active')),
                schema(integer_schema('id'), string_schema('name'))
            )
        );
    }

    public function test_schema_with_an_extra_entry() : void
    {
        self::assertTrue(
            (new SelectiveValidator())->isValid(
                schema(integer_schema('id'), string_schema('name'), bool_schema('active', true)),
                schema(integer_schema('id'), string_schema('name'))
            )
        );
    }

    public function test_schema_with_from_null_metadata() : void
    {
        self::assertTrue(
            (new SelectiveValidator())->isValid(
                schema(string_schema('id', nullable: true, metadata: Metadata::with(Metadata::FROM_NULL, true))),
                schema(integer_schema('id', nullable: true)),
            )
        );

        self::assertFalse(
            (new SelectiveValidator())->isValid(
                schema(string_schema('id', nullable: true)),
                schema(integer_schema('id', nullable: true)),
            )
        );
    }

    public function test_schema_with_multiple_columns_with_from_null_metadata() : void
    {
        self::assertTrue(
            (new SelectiveValidator())->isValid(
                given: schema(
                    string_schema('id', nullable: true, metadata: Metadata::with(Metadata::FROM_NULL, true)),
                    string_schema('name')
                ),
                expected: schema(
                    integer_schema('id', nullable: true),
                    string_schema('name')
                )
            )
        );

        self::assertFalse(
            (new SelectiveValidator())->isValid(
                given: schema(
                    string_schema('id', nullable: true),
                    string_schema('name')
                ),
                expected: schema(
                    integer_schema('id', nullable: true),
                    string_schema('name', nullable: true)
                )
            )
        );

        self::assertFalse(
            (new SelectiveValidator())->isValid(
                given: schema(
                    string_schema('id', nullable: true),
                    string_schema('name')
                ),
                expected: schema(
                    integer_schema('id', nullable: true),
                    string_schema('name')
                )
            )
        );
    }

    public function test_schema_with_single_invalid_column() : void
    {
        self::assertFalse(
            (new SelectiveValidator())->isValid(
                schema(integer_schema('id'), string_schema('name'), bool_schema('active', true)),
                schema(integer_schema('id'), bool_schema('name'), bool_schema('active'))
            )
        );
    }

    public function test_with_from_null_metadata_but_non_string_type() : void
    {
        self::assertFalse(
            (new SelectiveValidator())->isValid(
                given: schema(bool_schema('id', nullable: true, metadata: Metadata::with(Metadata::FROM_NULL, true))),
                expected: schema(integer_schema('id', nullable: true)),
            )
        );
    }
}
