<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Loader;

use Flow\ETL\Config;
use Flow\ETL\DSL\Entry;
use Flow\ETL\Exception\SchemaValidationException;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader\SchemaValidationLoader;
use Flow\ETL\Row;
use Flow\ETL\Row\Schema;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

final class SchemaValidationLoaderTest extends TestCase
{
    public function test_schema_validation_failed() : void
    {
        $this->expectException(SchemaValidationException::class);
        $this->expectExceptionMessage(
            <<<EXCEPTION
Given schema:
schema
|-- id: Flow\ETL\Row\Entry\IntegerEntry (nullable = false)

Does not match rows:
schema
|-- id: Flow\ETL\Row\Entry\StringEntry (nullable = false)

EXCEPTION
        );

        $loader = new SchemaValidationLoader(
            new Schema(
                Schema\Definition::integer('id')
            ),
            new Schema\StrictValidator()
        );

        $loader->load(new Rows(
            Row::create(Entry::string('id', '1'))
        ), new FlowContext(Config::default()));
    }

    public function test_schema_validation_succeed() : void
    {
        $loader = new SchemaValidationLoader(
            new Schema(
                Schema\Definition::integer('id')
            ),
            new Schema\StrictValidator()
        );

        $loader->load(new Rows(
            Row::create(Entry::integer('id', 1))
        ), new FlowContext(Config::default()));

        // validate that error wasn't thrown
        $this->assertTrue(true);
    }
}
