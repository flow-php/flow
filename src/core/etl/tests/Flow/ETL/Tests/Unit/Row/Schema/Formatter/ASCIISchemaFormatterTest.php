<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Schema\Formatter;

use Flow\ETL\PHP\Type\Logical\Structure\StructureElement;
use Flow\ETL\PHP\Type\Logical\StructureType;
use Flow\ETL\PHP\Type\Native\ScalarType;
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
            Schema\Definition::structure(
                'user',
                new StructureType(
                    new StructureElement('name', ScalarType::string(true)),
                    new StructureElement('age', ScalarType::integer()),
                    new StructureElement(
                        'address',
                        new StructureType(
                            new StructureElement('street', ScalarType::string(true)),
                            new StructureElement('city', ScalarType::string(true)),
                            new StructureElement('country', ScalarType::string(true)),
                        )
                    )
                ),
            ),
            Schema\Definition::string('name', nullable: true),
            Schema\Definition::array('tags', nullable: false),
            Schema\Definition::boolean('active', false),
            Schema\Definition::xml('xml', false)
        );

        $this->assertSame(
            <<<SCHEMA
schema
|-- user: Flow\ETL\Row\Entry\StructureEntry (nullable = false)
|    |-- name: ?string (nullable = true)
|    |-- age: integer (nullable = false)
|    |-- address: structure{street: ?string, city: ?string, country: ?string} (nullable = false)
|        |-- street: ?string (nullable = true)
|        |-- city: ?string (nullable = true)
|        |-- country: ?string (nullable = true)
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
