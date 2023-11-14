<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Schema\Formatter;

use Flow\ETL\PHP\Type\Logical\List\ListElement;
use Flow\ETL\PHP\Type\Logical\ListType;
use Flow\ETL\PHP\Type\Logical\Map\MapKey;
use Flow\ETL\PHP\Type\Logical\Map\MapValue;
use Flow\ETL\PHP\Type\Logical\MapType;
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
            Schema\Definition::union('number', [IntegerEntry::class, FloatEntry::class]),
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
            Schema\Definition::array('tags'),
            Schema\Definition::boolean('active'),
            Schema\Definition::xml('xml')
        );

        $this->assertSame(
            <<<'SCHEMA'
schema
|-- number: integer|float
|-- user: structure
|    |-- name: ?string
|    |-- age: integer
|    |-- address: structure
|        |-- street: ?string
|        |-- city: ?string
|        |-- country: ?string
|-- name: ?string
|-- tags: array<mixed>
|-- active: boolean
|-- xml: object<DOMDocument>

SCHEMA,
            (new ASCIISchemaFormatter())->format($schema)
        );
    }

    public function test_format_schema() : void
    {
        $schema = new Schema(
            Schema\Definition::union('number', [IntegerEntry::class, FloatEntry::class]),
            Schema\Definition::string('name', nullable: true),
            Schema\Definition::array('tags'),
            Schema\Definition::boolean('active'),
            Schema\Definition::xml('xml'),
            Schema\Definition::map('map', new MapType(MapKey::string(), MapValue::string())),
            Schema\Definition::list('list', new ListType(ListElement::map(new MapType(MapKey::string(), MapValue::integer()))))
        );

        $this->assertSame(
            <<<'SCHEMA'
schema
|-- number: integer|float
|-- name: ?string
|-- tags: array<mixed>
|-- active: boolean
|-- xml: object<DOMDocument>
|-- map: map<string, string>
|-- list: list<map<string, integer>>

SCHEMA,
            (new ASCIISchemaFormatter())->format($schema)
        );
    }
}
