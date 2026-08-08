<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema;

use Flow\ETL\Schema\Validator\MismatchedDefinition;
use Flow\ETL\Schema\Validator\SelectiveValidator;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\definition_from_type;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\integer_schema;
use function Flow\ETL\DSL\null_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\schema_selective_validator;
use function Flow\ETL\DSL\schema_validate;
use function Flow\ETL\DSL\string_schema;
use function Flow\Types\DSL\type_array;

final class SelectiveValidatorTest extends FlowTestCase
{
    public function test_given_schema_non_nullable_expected_nullable(): void
    {
        static::assertTrue(
            schema_validate(
                expected: schema(integer_schema('id'), string_schema('name'), bool_schema('active', true)),
                given: schema(integer_schema('id'), string_schema('name'), bool_schema('active')),
                validator: schema_selective_validator(),
            )->isValid(),
        );
    }

    public function test_context_collects_missing_and_mismatched_definitions_ignoring_extra_entries(): void
    {
        $context = schema_validate(
            expected: schema(integer_schema('id'), string_schema('name')),
            given: schema(string_schema('id'), bool_schema('extra')),
            validator: schema_selective_validator(),
        );

        static::assertFalse($context->isValid());
        static::assertEquals([string_schema('name')], $context->missingDefinitions());
        static::assertEquals(
            [new MismatchedDefinition(integer_schema('id'), string_schema('id'))],
            $context->mismatchedDefinitions(),
        );
        static::assertSame([], $context->unexpectedDefinitions());
    }

    public function test_given_schema_nullable_expected_non_nullable(): void
    {
        static::assertFalse(
            schema_validate(
                expected: schema(integer_schema('id'), string_schema('name'), bool_schema('active')),
                given: schema(integer_schema('id'), string_schema('name'), bool_schema('active', true)),
                validator: schema_selective_validator(),
            )->isValid(),
        );
    }

    public function test_given_schema_inferred_from_empty_arrays_against_declared_array_type(): void
    {
        static::assertTrue(
            schema_validate(
                expected: schema(integer_schema('id'), definition_from_type('a', type_array())),
                given: data_frame()->read(from_array([['id' => 1, 'a' => []], ['id' => 2, 'a' => []]]))->schema(),
                validator: schema_selective_validator(),
            )->isValid(),
        );
    }

    public function test_schema_with_a_missing_entry(): void
    {
        static::assertFalse(
            schema_validate(
                schema(integer_schema('id'), string_schema('name')),
                schema(integer_schema('id'), bool_schema('active')),
                schema_selective_validator(),
            )->isValid(),
        );
    }

    public function test_schema_with_an_extra_entry(): void
    {
        static::assertTrue(
            schema_validate(
                schema(integer_schema('id'), string_schema('name')),
                schema(integer_schema('id'), string_schema('name'), bool_schema('active', true)),
                schema_selective_validator(),
            )->isValid(),
        );
    }

    public function test_schema_with_null_definition(): void
    {
        static::assertTrue(
            (new SelectiveValidator())
                ->validate(schema(integer_schema('id', nullable: true)), schema(null_schema('id')))
                ->isValid(),
        );

        static::assertFalse(
            (new SelectiveValidator())
                ->validate(schema(integer_schema('id', nullable: true)), schema(string_schema('id', nullable: true)))
                ->isValid(),
        );
    }

    public function test_schema_with_multiple_columns_including_null_definition(): void
    {
        static::assertTrue(
            (new SelectiveValidator())
                ->validate(
                    expected: schema(integer_schema('id', nullable: true), string_schema('name')),
                    given: schema(null_schema('id'), string_schema('name')),
                )
                ->isValid(),
        );

        static::assertFalse(
            (new SelectiveValidator())
                ->validate(
                    expected: schema(integer_schema('id', nullable: true), string_schema('name', nullable: true)),
                    given: schema(string_schema('id', nullable: true), string_schema('name')),
                )
                ->isValid(),
        );

        static::assertFalse(
            (new SelectiveValidator())
                ->validate(
                    expected: schema(integer_schema('id', nullable: true), string_schema('name')),
                    given: schema(string_schema('id', nullable: true), string_schema('name')),
                )
                ->isValid(),
        );
    }

    public function test_schema_with_single_invalid_column(): void
    {
        static::assertFalse(
            (new SelectiveValidator())
                ->validate(
                    schema(integer_schema('id'), bool_schema('name'), bool_schema('active')),
                    schema(integer_schema('id'), string_schema('name'), bool_schema('active', true)),
                )
                ->isValid(),
        );
    }

    public function test_null_definition_against_non_nullable_expected(): void
    {
        static::assertFalse(
            (new SelectiveValidator())
                ->validate(expected: schema(integer_schema('id')), given: schema(null_schema('id')))
                ->isValid(),
        );
    }
}
