<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Loader;

use Flow\ETL\Exception\SchemaValidationException;
use Flow\ETL\Loader\SchemaValidationLoader;
use Flow\ETL\Schema\Validator\StrictValidator;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\integer_schema;
use function Flow\ETL\DSL\null_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
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

        $loader = new SchemaValidationLoader(schema(integer_schema('id')), new StrictValidator());

        $loader->load(rows(schema(str_schema('id')), row(['id' => '1'])), flow_context(config()));
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

        $loader = new SchemaValidationLoader(schema(integer_schema('id')), new StrictValidator());

        $loader->load(rows(schema(str_schema('name')), row(['name' => '1'])), flow_context(config()));
    }

    public function test_schema_validation_failure_message_lists_only_definitions_rejected_by_validator(): void
    {
        $loader = new SchemaValidationLoader(
            schema(string_schema('id'), datetime_schema('deleted_at', nullable: true)),
            new StrictValidator(),
        );

        try {
            $loader->load(
                rows(schema(int_schema('id'), null_schema('deleted_at')), row(['id' => 1, 'deleted_at' => null])),
                flow_context(config()),
            );
            static::fail('SchemaValidationException was not thrown');
        } catch (SchemaValidationException $exception) {
            static::assertStringContainsString('expected: id<string>, given: id<integer>', $exception->getMessage());
            static::assertStringNotContainsString('deleted_at', $exception->getMessage());
        }
    }

    public function test_schema_validation_succeed(): void
    {
        $loader = new SchemaValidationLoader(schema(integer_schema('id')), new StrictValidator());

        $loader->load(rows(schema(int_schema('id')), row(['id' => 1])), flow_context(config()));

        // validate that error wasn't thrown
        $this->addToAssertionCount(1);
    }
}
