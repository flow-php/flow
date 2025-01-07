<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Loader;

use function Flow\ETL\DSL\{config, row, rows};
use function Flow\ETL\DSL\{flow_context, integer_schema, schema};
use function Flow\ETL\DSL\{int_entry, str_entry};
use Flow\ETL\Exception\SchemaValidationException;
use Flow\ETL\Loader\SchemaValidationLoader;
use Flow\ETL\Row\Schema\StrictValidator;
use Flow\ETL\{Tests\FlowTestCase};

final class SchemaValidationLoaderTest extends FlowTestCase
{
    public function test_schema_validation_failed() : void
    {
        $this->expectException(SchemaValidationException::class);
        $this->expectExceptionMessage(
            <<<'EXCEPTION'
Given schema:
schema
|-- id: integer

Does not match rows:
schema
|-- id: string

EXCEPTION
        );

        $loader = new SchemaValidationLoader(
            schema(integer_schema('id')),
            new StrictValidator()
        );

        $loader->load(rows(row(str_entry('id', '1'))), flow_context(config()));
    }

    public function test_schema_validation_succeed() : void
    {
        $loader = new SchemaValidationLoader(
            schema(integer_schema('id')),
            new StrictValidator()
        );

        $loader->load(rows(row(int_entry('id', 1))), flow_context(config()));

        // validate that error wasn't thrown
        $this->addToAssertionCount(1);
    }
}
