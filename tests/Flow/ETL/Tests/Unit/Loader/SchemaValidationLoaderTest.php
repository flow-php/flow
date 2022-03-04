<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Loader;

use Flow\ETL\DSL\Entry;
use Flow\ETL\Exception\SchemaValidationException;
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

        $loader = new SchemaValidationLoader(
            new Schema(
                Schema\Definition::integer('id')
            ),
            new Schema\StrictValidator()
        );

        $loader->load(new Rows(
            Row::create(Entry::string('id', '1'))
        ));
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
        ));

        // validate that error wasn't thrown
        $this->assertTrue(true);
    }
}
