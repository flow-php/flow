<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\schema_evolving_validator;
use function Flow\ETL\DSL\schema_validate;
use function Flow\ETL\DSL\str_schema;

final class EvolvingValidatorTest extends FlowTestCase
{
    public function test_given_having_less_definitions_than_expected(): void
    {
        $expected = schema(int_schema('id'), str_schema('name'));

        $given = schema(int_schema('id'));

        static::assertFalse(schema_validate($expected, $given, schema_evolving_validator()));
    }

    public function test_given_having_same_number_of_definitions_but_different_names(): void
    {
        $expected = schema(int_schema('id'), str_schema('name'));

        $given = schema(int_schema('id'), str_schema('surname'));

        static::assertFalse(schema_validate($expected, $given, schema_evolving_validator()));
    }

    public function test_given_schema_adding_new_field(): void
    {
        $expected = schema(int_schema('id'), str_schema('name'));

        $given = schema(int_schema('id'), str_schema('name'), bool_schema('active'));

        static::assertTrue(schema_validate($expected, $given, schema_evolving_validator()));
    }

    public function test_given_schema_changing_nullable_field_to_non_nullable(): void
    {
        $expected = schema(int_schema('id'), str_schema('name', nullable: true));

        $given = schema(int_schema('id'), str_schema('name'));

        static::assertFalse(schema_validate($expected, $given, schema_evolving_validator()));
    }

    public function test_given_schema_changing_type_of_field(): void
    {
        $expected = schema(int_schema('id'), str_schema('name'));

        $given = schema(int_schema('id'), bool_schema('name'));

        static::assertFalse(schema_validate($expected, $given, schema_evolving_validator()));
    }

    public function test_given_schema_is_the_same_as_expected_schema(): void
    {
        $expected = schema(int_schema('id'), str_schema('name'));

        $given = schema(int_schema('id'), str_schema('name'));

        static::assertTrue(schema_validate($expected, $given, schema_evolving_validator()));
    }

    public function test_given_schema_making_non_nullable_field_into_nullable(): void
    {
        $expected = schema(int_schema('id'), str_schema('name'));

        $given = schema(int_schema('id'), str_schema('name', nullable: true));

        static::assertTrue(schema_validate($expected, $given, schema_evolving_validator()));
    }

    public function test_given_totally_different(): void
    {
        $expected = schema(int_schema('id'), str_schema('name'));

        $given = schema(int_schema('not_id'), str_schema('surname'), bool_schema('active'));

        static::assertFalse(schema_validate($expected, $given, schema_evolving_validator()));
    }
}
