<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema;

use Flow\ETL\Schema\Metadata;
use Flow\ETL\Schema\Validator\MismatchedDefinition;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\integer_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\schema_strict_validator;
use function Flow\ETL\DSL\schema_validate;
use function Flow\ETL\DSL\string_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\ETL\DSL\union_schema;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_union;

final class StrictValidatorTest extends FlowTestCase
{
    public function test_given_schema_non_nullable_expected_nullable(): void
    {
        static::assertTrue(
            schema_validate(
                expected: schema(integer_schema('id'), string_schema('name'), bool_schema('active', true)),
                given: schema(integer_schema('id'), string_schema('name'), bool_schema('active')),
                validator: schema_strict_validator(),
            )->isValid(),
        );
    }

    public function test_given_schema_nullable_expected_non_nullable(): void
    {
        static::assertFalse(
            schema_validate(
                expected: schema(integer_schema('id'), string_schema('name'), bool_schema('active')),
                given: schema(integer_schema('id'), string_schema('name'), bool_schema('active', true)),
                validator: schema_strict_validator(),
            )->isValid(),
        );
    }

    public function test_context_collects_missing_mismatched_and_unexpected_definitions(): void
    {
        $context = schema_validate(
            expected: schema(integer_schema('id'), string_schema('name'), bool_schema('active')),
            given: schema(string_schema('id'), bool_schema('extra')),
            validator: schema_strict_validator(),
        );

        static::assertFalse($context->isValid());
        static::assertEquals([string_schema('name'), bool_schema('active')], $context->missingDefinitions());
        static::assertEquals(
            [new MismatchedDefinition(integer_schema('id'), string_schema('id'))],
            $context->mismatchedDefinitions(),
        );
        static::assertEquals([bool_schema('extra')], $context->unexpectedDefinitions());
    }

    public function test_context_collects_mismatch_hidden_deep_in_nested_structure(): void
    {
        $expectedProfile = structure_schema('profile', type_structure([
            'name' => type_string(),
            'addresses' => type_list(type_structure([
                'street' => type_string(),
                'geo' => type_map(type_string(), type_float()),
            ])),
        ]));
        $givenProfile = structure_schema('profile', type_structure([
            'name' => type_string(),
            'addresses' => type_list(type_structure([
                'street' => type_string(),
                'geo' => type_map(type_string(), type_string()),
            ])),
        ]));

        $context = schema_validate(
            expected: schema(integer_schema('id'), $expectedProfile),
            given: schema(integer_schema('id'), $givenProfile),
            validator: schema_strict_validator(),
        );

        static::assertFalse($context->isValid());
        static::assertEquals(
            [new MismatchedDefinition($expectedProfile, $givenProfile)],
            $context->mismatchedDefinitions(),
        );
        static::assertSame([], $context->missingDefinitions());
        static::assertSame([], $context->unexpectedDefinitions());
    }

    public function test_given_schema_with_matching_union_definition(): void
    {
        static::assertTrue(
            schema_validate(
                expected: schema(
                    integer_schema('id'),
                    union_schema('value', type_union(type_string(), type_integer())),
                ),
                given: schema(integer_schema('id'), union_schema('value', type_union(type_string(), type_integer()))),
                validator: schema_strict_validator(),
            )->isValid(),
        );
    }

    public function test_given_schema_with_mismatched_union_definition(): void
    {
        $context = schema_validate(
            expected: schema(union_schema('value', type_union(type_string(), type_integer()))),
            given: schema(string_schema('value')),
            validator: schema_strict_validator(),
        );

        static::assertFalse($context->isValid());
        static::assertEquals(
            [new MismatchedDefinition(
                union_schema('value', type_union(type_string(), type_integer())),
                string_schema('value'),
            )],
            $context->mismatchedDefinitions(),
        );
    }

    public function test_rows_with_a_missing_entry(): void
    {
        static::assertFalse(
            schema_validate(
                expected: schema(integer_schema('id'), string_schema('name'), bool_schema('active', true)),
                given: schema(integer_schema('id'), string_schema('name')),
                validator: schema_strict_validator(),
            )->isValid(),
        );
    }

    public function test_rows_with_an_extra_entry(): void
    {
        static::assertFalse(
            schema_validate(
                expected: schema(integer_schema('id'), string_schema('name'), bool_schema('active')),
                given: schema(
                    integer_schema('id'),
                    string_schema('name'),
                    bool_schema('active'),
                    list_schema('tags', type_list(type_string())),
                ),
                validator: schema_strict_validator(),
            )->isValid(),
        );
    }

    public function test_rows_with_from_null_metadata(): void
    {
        static::assertTrue(
            schema_validate(
                expected: schema(integer_schema('id', nullable: true)),
                given: schema(string_schema('id', nullable: true, metadata: Metadata::with(Metadata::FROM_NULL, true))),
                validator: schema_strict_validator(),
            )->isValid(),
        );

        static::assertFalse(
            schema_validate(
                expected: schema(string_schema('id', nullable: true)),
                given: schema(integer_schema('id', nullable: true)),
                validator: schema_strict_validator(),
            )->isValid(),
        );
    }

    public function test_rows_with_multiple_columns_with_from_null_metadata(): void
    {
        static::assertFalse(
            schema_validate(
                expected: schema(integer_schema('id', nullable: true), string_schema('name')),
                given: schema(string_schema('id', nullable: true), string_schema('name')),
                validator: schema_strict_validator(),
            )->isValid(),
        );

        static::assertTrue(
            schema_validate(
                expected: schema(integer_schema('id', nullable: true), string_schema('name')),
                given: schema(
                    string_schema('id', nullable: true, metadata: Metadata::with(Metadata::FROM_NULL, true)),
                    string_schema('name'),
                ),
                validator: schema_strict_validator(),
            )->isValid(),
        );
    }

    public function test_with_from_null_metadata_but_non_string_type(): void
    {
        static::assertFalse(
            schema_validate(
                expected: schema(integer_schema('id', nullable: true)),
                given: schema(bool_schema('id', nullable: true, metadata: Metadata::with(Metadata::FROM_NULL, true))),
                validator: schema_strict_validator(),
            )->isValid(),
        );
    }
}
