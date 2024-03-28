<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Schema\Matcher;

use function Flow\ETL\DSL\{bool_schema, int_schema, schema, str_schema};
use Flow\ETL\Row\Schema\Matcher\EvolvingSchemaMatcher;
use PHPUnit\Framework\TestCase;

final class EvolvingSchemaMatcherTest extends TestCase
{
    public function test_right_having_less_definitions_than_left() : void
    {
        $left = schema(
            int_schema('id'),
            str_schema('name'),
        );

        $right = schema(
            int_schema('id'),
        );

        self::assertFalse((new EvolvingSchemaMatcher())->match($left, $right));
    }

    public function test_right_having_same_number_of_definitions_but_different_names() : void
    {
        $left = schema(
            int_schema('id'),
            str_schema('name'),
        );

        $right = schema(
            int_schema('id'),
            str_schema('surname'),
        );

        self::assertFalse((new EvolvingSchemaMatcher())->match($left, $right));
    }

    public function test_right_schema_adding_new_field() : void
    {
        $left = schema(
            int_schema('id'),
            str_schema('name'),
        );

        $right = schema(
            int_schema('id'),
            str_schema('name'),
            bool_schema('active'),
        );

        self::assertTrue((new EvolvingSchemaMatcher())->match($left, $right));
    }

    public function test_right_schema_changing_nullable_field_to_non_nullable() : void
    {
        $left = schema(
            int_schema('id'),
            str_schema('name', nullable: true),
        );

        $right = schema(
            int_schema('id'),
            str_schema('name'),
        );

        self::assertFalse((new EvolvingSchemaMatcher())->match($left, $right));
    }

    public function test_right_schema_changing_type_of_field() : void
    {
        $left = schema(
            int_schema('id'),
            str_schema('name'),
        );

        $right = schema(
            int_schema('id'),
            bool_schema('name'),
        );

        self::assertFalse((new EvolvingSchemaMatcher())->match($left, $right));
    }

    public function test_right_schema_is_the_same_as_left_schema() : void
    {
        $left = schema(
            int_schema('id'),
            str_schema('name'),
        );

        $right = schema(
            int_schema('id'),
            str_schema('name'),
        );

        self::assertTrue((new EvolvingSchemaMatcher())->match($left, $right));
    }

    public function test_right_schema_making_non_nullable_field_into_nullable() : void
    {
        $left = schema(
            int_schema('id'),
            str_schema('name'),
        );

        $right = schema(
            int_schema('id'),
            str_schema('name', nullable: true),
        );

        self::assertTrue((new EvolvingSchemaMatcher())->match($left, $right));
    }
}
