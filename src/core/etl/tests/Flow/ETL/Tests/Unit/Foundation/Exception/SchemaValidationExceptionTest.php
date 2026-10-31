<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Foundation\Exception;

use Flow\ETL\Foundation\Exception\SchemaValidationException;
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
        $exception = new SchemaValidationException(
            schema_validate(
                schema(integer_schema('id'), string_schema('name'), string_schema('email')),
                schema(
                    integer_schema('id'),
                    string_schema('name'),
                    string_schema('email', true),
                    string_schema('address'),
                ),
            )->toString(),
        );

        static::assertStringContainsString(
            "Schema validation failed: \n"
            . "  Mismatched Definitions: \n"
            . "    |-- expected: email<string>, given: email<?string>\n"
            . "  Unexpected Definitions: \n"
            . '    |-- address<string>',
            $exception->getMessage(),
        );
    }

    public function test_diff_message_does_not_list_from_null_definitions_accepted_by_validator(): void
    {
        $exception = new SchemaValidationException(
            schema_validate(
                schema(string_schema('id'), datetime_schema('deleted_at', nullable: true)),
                schema(integer_schema('id'), null_schema('deleted_at')),
            )->toString(),
        );

        static::assertStringContainsString('expected: id<string>, given: id<integer>', $exception->getMessage());
        static::assertStringNotContainsString('deleted_at', $exception->getMessage());
    }

    public function test_diff_message_renders_nullable_marker_on_expected_type(): void
    {
        static::assertStringContainsString(
            'expected: deleted_at<?datetime>, given: deleted_at<integer>',
            (new SchemaValidationException(
                schema_validate(
                    schema(datetime_schema('deleted_at', nullable: true)),
                    schema(integer_schema('deleted_at')),
                )->toString(),
            ))->getMessage(),
        );
    }

    public function test_the_message_prefixes_the_mismatches(): void
    {
        static::assertSame(
            "Schema validation failed: \n  Unexpected Definitions: \n",
            (new SchemaValidationException("  Unexpected Definitions: \n"))->getMessage(),
        );
    }

    public function test_declared_columns_missing_names_them_the_source_its_header_and_the_fix(): void
    {
        static::assertSame(
            "Schema validation failed: \n"
            . 'Columns "id", "at" declared by the schema are not in the header of file:///a.csv (name, price). Correct '
            . 'their names, or remove them from ->withSchema(...).',
            SchemaValidationException::declaredColumnsMissing(
                'file:///a.csv',
                ['id', 'at'],
                ['name', 'price'],
            )->getMessage(),
        );
    }
}
