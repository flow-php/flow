<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Schema\Formatter;

use Flow\ETL\Row\Entry\FloatEntry;
use Flow\ETL\Row\Entry\IntegerEntry;
use Flow\ETL\Row\Schema;
use Flow\ETL\Row\Schema\Formatter\ASCIISchemaFormatter;
use PHPUnit\Framework\TestCase;

final class ASCIISchemaFormatterTest extends TestCase
{
    public function test_format_nested_schema() : void
    {
        $schema = new Schema(
            Schema\Definition::structure('user', [
                Schema\Definition::string('name', nullable: true),
                Schema\Definition::integer('age', nullable: false),
                Schema\Definition::structure('address', [
                    Schema\Definition::string('street', nullable: true),
                    Schema\Definition::string('city', nullable: true),
                    Schema\Definition::string('country', nullable: true),
                ]),
            ]),
            Schema\Definition::string('name', nullable: true),
            Schema\Definition::array('tags', nullable: false),
            Schema\Definition::boolean('active', false),
            Schema\Definition::xml('xml', false)
        );

        $this->assertSame(
            <<<SCHEMA
schema
|-- user: Flow\ETL\Row\Entry\StructureEntry (nullable = false)
|    |-- name: [Flow\ETL\Row\Entry\StringEntry, Flow\ETL\Row\Entry\NullEntry] (nullable = true)
|    |-- age: Flow\ETL\Row\Entry\IntegerEntry (nullable = false)
|    |-- address: Flow\ETL\Row\Entry\StructureEntry (nullable = false)
|        |-- street: [Flow\ETL\Row\Entry\StringEntry, Flow\ETL\Row\Entry\NullEntry] (nullable = true)
|        |-- city: [Flow\ETL\Row\Entry\StringEntry, Flow\ETL\Row\Entry\NullEntry] (nullable = true)
|        |-- country: [Flow\ETL\Row\Entry\StringEntry, Flow\ETL\Row\Entry\NullEntry] (nullable = true)
|-- name: [Flow\ETL\Row\Entry\StringEntry, Flow\ETL\Row\Entry\NullEntry] (nullable = true)
|-- tags: Flow\ETL\Row\Entry\ArrayEntry (nullable = false)
|-- active: Flow\ETL\Row\Entry\BooleanEntry (nullable = false)
|-- xml: Flow\ETL\Row\Entry\XMLEntry (nullable = false)

SCHEMA,
            (new ASCIISchemaFormatter())->format($schema)
        );
    }

    public function test_format_schema() : void
    {
        $schema = new Schema(
            Schema\Definition::union('number', [IntegerEntry::class, FloatEntry::class]),
            Schema\Definition::string('name', nullable: true),
            Schema\Definition::array('tags', nullable: false),
            Schema\Definition::boolean('active', false),
            Schema\Definition::xml('xml', false)
        );

        $this->assertSame(
            <<<SCHEMA
schema
|-- number: [Flow\ETL\Row\Entry\IntegerEntry, Flow\ETL\Row\Entry\FloatEntry] (nullable = false)
|-- name: [Flow\ETL\Row\Entry\StringEntry, Flow\ETL\Row\Entry\NullEntry] (nullable = true)
|-- tags: Flow\ETL\Row\Entry\ArrayEntry (nullable = false)
|-- active: Flow\ETL\Row\Entry\BooleanEntry (nullable = false)
|-- xml: Flow\ETL\Row\Entry\XMLEntry (nullable = false)

SCHEMA,
            (new ASCIISchemaFormatter())->format($schema)
        );
    }
}
