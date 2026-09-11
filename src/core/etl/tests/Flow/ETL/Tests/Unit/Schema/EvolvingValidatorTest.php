<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema;

use Flow\ETL\Schema\Validator\MismatchedDefinition;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\null_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\schema_evolving_validator;
use function Flow\ETL\DSL\schema_validate;
use function Flow\ETL\DSL\str_schema;

final class EvolvingValidatorTest extends FlowTestCase
{
    public function test_adding_a_new_non_nullable_column_is_invalid(): void
    {
        $expected = schema(int_schema('id'), str_schema('name'));

        $given = schema(int_schema('id'), str_schema('name'), bool_schema('active'));

        static::assertFalse(schema_validate($expected, $given, schema_evolving_validator())->isValid());
    }

    public function test_adding_a_new_nullable_column_is_valid(): void
    {
        $expected = schema(int_schema('id'), str_schema('name'));

        $given = schema(int_schema('id'), str_schema('name'), bool_schema('active', nullable: true));

        static::assertTrue(schema_validate($expected, $given, schema_evolving_validator())->isValid());
    }

    public function test_context_collects_missing_mismatched_and_unexpected_required_definitions(): void
    {
        $context = schema_validate(
            expected: schema(int_schema('id'), str_schema('name', nullable: true), bool_schema('active')),
            given: schema(str_schema('name'), str_schema('active'), bool_schema('extra')),
            validator: schema_evolving_validator(),
        );

        static::assertFalse($context->isValid());
        static::assertEquals([int_schema('id')], $context->missingDefinitions());
        static::assertEquals(
            [new MismatchedDefinition(bool_schema('active'), str_schema('active'))],
            $context->mismatchedDefinitions(),
        );
        static::assertEquals([bool_schema('extra')], $context->unexpectedDefinitions());
    }

    public function test_omitting_a_non_nullable_column_is_invalid(): void
    {
        $expected = schema(int_schema('id'), str_schema('name'));

        $given = schema(int_schema('id'));

        static::assertFalse(schema_validate($expected, $given, schema_evolving_validator())->isValid());
    }

    public function test_omitting_a_nullable_column_is_valid(): void
    {
        $expected = schema(int_schema('id'), str_schema('name', nullable: true));

        $given = schema(int_schema('id'));

        static::assertTrue(schema_validate($expected, $given, schema_evolving_validator())->isValid());
    }

    public function test_given_having_same_number_of_definitions_but_different_names(): void
    {
        $expected = schema(int_schema('id'), str_schema('name'));

        $given = schema(int_schema('id'), str_schema('surname'));

        static::assertFalse(schema_validate($expected, $given, schema_evolving_validator())->isValid());
    }

    public function test_narrowing_a_nullable_expected_column_to_non_nullable_is_valid(): void
    {
        $expected = schema(int_schema('id'), str_schema('name', nullable: true));

        $given = schema(int_schema('id'), str_schema('name'));

        static::assertTrue(schema_validate($expected, $given, schema_evolving_validator())->isValid());
    }

    public function test_widening_a_non_nullable_expected_column_to_nullable_is_invalid(): void
    {
        $expected = schema(int_schema('id'), str_schema('name'));

        $given = schema(int_schema('id'), str_schema('name', nullable: true));

        static::assertFalse(schema_validate($expected, $given, schema_evolving_validator())->isValid());
    }

    public function test_changing_the_type_of_a_column_is_invalid(): void
    {
        $expected = schema(int_schema('id'), str_schema('name'));

        $given = schema(int_schema('id'), bool_schema('name'));

        static::assertFalse(schema_validate($expected, $given, schema_evolving_validator())->isValid());
    }

    public function test_identical_schema_is_valid(): void
    {
        $expected = schema(int_schema('id'), str_schema('name'));

        $given = schema(int_schema('id'), str_schema('name'));

        static::assertTrue(schema_validate($expected, $given, schema_evolving_validator())->isValid());
    }

    public function test_totally_different_schema_is_invalid(): void
    {
        $expected = schema(int_schema('id'), str_schema('name'));

        $given = schema(int_schema('not_id'), str_schema('surname'), bool_schema('active'));

        static::assertFalse(schema_validate($expected, $given, schema_evolving_validator())->isValid());
    }

    public function test_an_all_null_batch_is_representable_under_a_nullable_declaration(): void
    {
        // B27: the other two validators already accept this; the evolving one had no NullDefinition arm
        static::assertTrue(
            schema_evolving_validator()
                ->validate(schema(str_schema('name', nullable: true)), schema(null_schema('name')))
                ->isValid(),
        );
    }

    public function test_an_all_null_batch_is_still_refused_under_a_not_null_declaration(): void
    {
        static::assertFalse(
            schema_evolving_validator()->validate(schema(str_schema('name')), schema(null_schema('name')))->isValid(),
        );
    }
}
