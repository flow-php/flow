<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Loader;

use Flow\ETL\Exception\SchemaValidationException;
use Flow\ETL\Loader\SchemaValidationLoader;
use Flow\ETL\Schema\Validator\StrictValidator;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\integer_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_entry;

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

        $loader->load(rows(row(str_entry('id', '1'))), flow_context(config()));
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

        $loader->load(rows(row(str_entry('name', '1'))), flow_context(config()));
    }

    public function test_schema_validation_succeed(): void
    {
        $loader = new SchemaValidationLoader(schema(integer_schema('id')), new StrictValidator());

        $loader->load(rows(row(int_entry('id', 1))), flow_context(config()));

        // validate that error wasn't thrown
        $this->addToAssertionCount(1);
    }
}
