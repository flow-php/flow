<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Exception;

use Flow\ETL\Exception\SchemaValidationException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\integer_schema;
use function Flow\ETL\DSL\null_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\schema_validate;
use function Flow\ETL\DSL\string_schema;

final class SchemaValidationExceptionTest extends FlowTestCase
{
    public function test_diff_message(): void
    {
        $expected = schema(integer_schema('id'), string_schema('name'), string_schema('email'));

        $given = schema(
            integer_schema('id'),
            string_schema('name'),
            string_schema('email', true),
            string_schema('address'),
        );

        $exception = new SchemaValidationException($expected, $given, schema_validate($expected, $given));

        static::assertStringContainsString(<<<'EXCEPTION'
            Schema validation failed: 
              Mismatched Definitions: 
                |-- expected: email<string>, given: email<?string>
              Unexpected Definitions: 
                |-- address<string>
            EXCEPTION, $exception->getMessage());
    }

    public function test_diff_message_does_not_list_from_null_definitions_accepted_by_validator(): void
    {
        $expected = schema(string_schema('id'), datetime_schema('deleted_at', nullable: true));
        $given = schema(integer_schema('id'), null_schema('deleted_at'));

        $exception = new SchemaValidationException($expected, $given, schema_validate($expected, $given));

        static::assertStringContainsString('expected: id<string>, given: id<integer>', $exception->getMessage());
        static::assertStringNotContainsString('deleted_at', $exception->getMessage());
    }

    public function test_diff_message_renders_nullable_marker_on_expected_type(): void
    {
        $expected = schema(datetime_schema('deleted_at', nullable: true));
        $given = schema(integer_schema('deleted_at'));

        $exception = new SchemaValidationException($expected, $given, schema_validate($expected, $given));

        static::assertStringContainsString(
            'expected: deleted_at<?datetime>, given: deleted_at<integer>',
            $exception->getMessage(),
        );
    }
}
