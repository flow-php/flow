<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Schema;

use function Flow\ETL\DSL\{bool_schema, schema};
use function Flow\ETL\DSL\{int_schema, schema_evolving_validator, schema_validate, str_schema};
use Flow\ETL\{Tests\FlowTestCase};

final class EvolvingValidatorTest extends FlowTestCase
{
    public function test_given_having_less_definitions_than_expected() : void
    {
        $expected = schema(
            int_schema('id'),
            str_schema('name'),
        );

        $given = schema(
            int_schema('id'),
        );

        self::assertFalse(schema_validate($expected, $given, schema_evolving_validator()));
    }

    public function test_given_having_same_number_of_definitions_but_different_names() : void
    {
        $expected = schema(
            int_schema('id'),
            str_schema('name'),
        );

        $given = schema(
            int_schema('id'),
            str_schema('surname'),
        );

        self::assertFalse(schema_validate($expected, $given, schema_evolving_validator()));
    }

    public function test_given_schema_adding_new_field() : void
    {
        $expected = schema(
            int_schema('id'),
            str_schema('name'),
        );

        $given = schema(
            int_schema('id'),
            str_schema('name'),
            bool_schema('active'),
        );

        self::assertTrue(schema_validate($expected, $given, schema_evolving_validator()));
    }

    public function test_given_schema_changing_nullable_field_to_non_nullable() : void
    {
        $expected = schema(
            int_schema('id'),
            str_schema('name', nullable: true),
        );

        $given = schema(
            int_schema('id'),
            str_schema('name'),
        );

        self::assertFalse(schema_validate($expected, $given, schema_evolving_validator()));
    }

    public function test_given_schema_changing_type_of_field() : void
    {
        $expected = schema(
            int_schema('id'),
            str_schema('name'),
        );

        $given = schema(
            int_schema('id'),
            bool_schema('name'),
        );

        self::assertFalse(schema_validate($expected, $given, schema_evolving_validator()));
    }

    public function test_given_schema_is_the_same_as_expected_schema() : void
    {
        $expected = schema(
            int_schema('id'),
            str_schema('name'),
        );

        $given = schema(
            int_schema('id'),
            str_schema('name'),
        );

        self::assertTrue(schema_validate($expected, $given, schema_evolving_validator()));
    }

    public function test_given_schema_making_non_nullable_field_into_nullable() : void
    {
        $expected = schema(
            int_schema('id'),
            str_schema('name'),
        );

        $given = schema(
            int_schema('id'),
            str_schema('name', nullable: true),
        );

        self::assertTrue(schema_validate($expected, $given, schema_evolving_validator()));
    }

    public function test_given_totally_different() : void
    {
        $expected = schema(
            int_schema('id'),
            str_schema('name'),
        );

        $given = schema(
            int_schema('not_id'),
            str_schema('surname'),
            bool_schema('active'),
        );

        self::assertFalse(schema_validate($expected, $given, schema_evolving_validator()));
    }
}
