<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Schema\Matcher;

use function Flow\ETL\DSL\{int_schema, schema, str_schema};
use Flow\ETL\Row\Schema\Matcher\StrictSchemaMatcher;
use Flow\ETL\Tests\FlowTestCase;

final class StrictSchemaMatcherTest extends FlowTestCase
{
    public function test_matching_different_schemas() : void
    {
        $left = schema(
            str_schema('id'),
            str_schema('name'),
        );

        $right = schema(
            str_schema('id'),
            str_schema('name'),
            int_schema('age'),
        );

        self::assertFalse((new StrictSchemaMatcher())->match($left, $right));
    }

    public function test_matching_same_number_of_definitions_but_different_names() : void
    {
        $left = schema(
            str_schema('id'),
            str_schema('name'),
        );

        $right = schema(
            str_schema('id'),
            str_schema('surname'),
        );

        self::assertFalse((new StrictSchemaMatcher())->match($left, $right));
    }

    public function test_matching_schemas_with_different_nullable_fields() : void
    {
        $left = schema(
            str_schema('id'),
            str_schema('name'),
        );

        $right = schema(
            str_schema('id'),
            str_schema('name', nullable: true),
        );

        self::assertFalse((new StrictSchemaMatcher())->match($left, $right));
    }

    public function test_matching_the_same_schema() : void
    {
        $left = schema(
            str_schema('id'),
            str_schema('name'),
        );

        $right = schema(
            str_schema('id'),
            str_schema('name'),
        );

        self::assertTrue((new StrictSchemaMatcher())->match($left, $right));
    }
}
