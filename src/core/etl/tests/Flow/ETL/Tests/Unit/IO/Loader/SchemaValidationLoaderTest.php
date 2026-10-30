<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\IO\Loader;

use Flow\ETL\Foundation\Exception\SchemaValidationException;
use Flow\ETL\IO\Loader\SchemaValidationLoader;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Schema;
use Flow\ETL\Values\Schema\Validator\StrictValidator;
use Flow\ETL\Values\Schema\Validator\ValidationContext;
use Flow\ETL\Values\SchemaValidator;
use RuntimeException;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\integer_schema;
use function Flow\ETL\DSL\null_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\string_schema;

final class SchemaValidationLoaderTest extends FlowTestCase
{
    public function test_schema_validation_failed_by_mismatching(): void
    {
        $this->expectException(SchemaValidationException::class);
        $this->expectExceptionMessage(<<<'EXCEPTION'
            Schema validation failed: 
              Mismatched Definitions: 
                |-- expected: id<integer>, given: id<string>
            EXCEPTION);

        (new SchemaValidationLoader(schema(integer_schema('id')), new StrictValidator()))->load(array_to_rows([[
            'id' => '1',
        ]], schema(str_schema('id'))), flow_context(config())->backend());
    }

    public function test_schema_validation_failed_by_unexpected(): void
    {
        $this->expectException(SchemaValidationException::class);
        $this->expectExceptionMessage(<<<'EXCEPTION'
            Schema validation failed: 
              Missing Definitions: 
                |-- id<integer>
              Unexpected Definitions: 
                |-- name<string>
            EXCEPTION);

        (new SchemaValidationLoader(schema(integer_schema('id')), new StrictValidator()))->load(array_to_rows([[
            'name' => '1',
        ]], schema(str_schema('name'))), flow_context(config())->backend());
    }

    public function test_schema_validation_failure_message_lists_only_definitions_rejected_by_validator(): void
    {
        try {
            (new SchemaValidationLoader(
                schema(string_schema('id'), datetime_schema('deleted_at', nullable: true)),
                new StrictValidator(),
            ))->load(
                array_to_rows([['id' => 1, 'deleted_at' => null]], schema(int_schema('id'), null_schema('deleted_at'))),
                flow_context(config())->backend(),
            );
            static::fail('SchemaValidationException was not thrown');
        } catch (SchemaValidationException $exception) {
            static::assertStringContainsString('expected: id<string>, given: id<integer>', $exception->getMessage());
            static::assertStringNotContainsString('deleted_at', $exception->getMessage());
        }
    }

    public function test_schema_validation_succeed(): void
    {
        (new SchemaValidationLoader(schema(integer_schema('id')), new StrictValidator()))->load(array_to_rows([[
            'id' => 1,
        ]], schema(int_schema('id'))), flow_context(config())->backend());

        // validate that error wasn't thrown
        $this->addToAssertionCount(1);
    }

    public function test_a_custom_validator_does_not_describe(): void
    {
        static::assertNull((new SchemaValidationLoader(schema(int_schema('id')), new class implements SchemaValidator {
            public function validate(Schema $expected, Schema $given): ValidationContext
            {
                throw new RuntimeException('never called');
            }
        }))->describe());
    }
}
