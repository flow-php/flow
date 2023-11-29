<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type;

use function Flow\ETL\DSL\type_int;
use function Flow\ETL\DSL\type_null;
use function Flow\ETL\DSL\type_string;
use Flow\ETL\PHP\Type\ArrayContentDetector;
use Flow\ETL\PHP\Type\Logical\List\ListElement;
use Flow\ETL\PHP\Type\Logical\ListType;
use Flow\ETL\PHP\Type\Logical\Map\MapKey;
use Flow\ETL\PHP\Type\Logical\Map\MapValue;
use Flow\ETL\PHP\Type\Logical\MapType;
use Flow\ETL\PHP\Type\Logical\Structure\StructureElement;
use Flow\ETL\PHP\Type\Logical\StructureType;
use Flow\ETL\PHP\Type\Native\ArrayType;
use Flow\ETL\PHP\Type\Types;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class ArrayContentDetectorTest extends TestCase
{
    public static function provide_list_data() : \Generator
    {
        yield 'simple list' => [
            [
                type_int(),
            ],
            [
                type_string(),
            ],
            true,
        ];

        yield 'simple map' => [
            [
                type_string(),
            ],
            [
                type_string(),
            ],
            false,
        ];

        yield 'simple structure' => [
            [
                type_string(),
            ],
            [
                type_string(),
                new MapType(MapKey::string(), MapValue::string()),
                new ListType(ListElement::integer()),
            ],
            false,
        ];

        yield 'list of unique same structures' => [
            [
                type_int(),
            ],
            [
                new StructureType(
                    new StructureElement('id', type_int()),
                    new StructureElement('name', type_string())
                ),
            ],
            true,
        ];

        yield 'map with string key, of maps string with string' => [
            [
                type_string(),
            ],
            [
                new MapType(
                    MapKey::string(),
                    MapValue::map(
                        new MapType(MapKey::string(), MapValue::string()),
                    )
                ),
            ],
            false,
        ];

        yield 'array of nulls' => [
            [
                type_string(),
            ],
            [
                type_null(),
                type_null(),
                type_null(),
            ],
            false,
        ];
    }

    public static function provide_map_data() : \Generator
    {
        yield 'simple list' => [
            [
                type_int(),
            ],
            [
                type_string(),
            ],
            false,
        ];

        yield 'simple map' => [
            [
                type_string(),
            ],
            [
                type_string(),
            ],
            true,
        ];

        yield 'simple structure' => [
            [
                type_string(),
            ],
            [
                type_string(),
                new MapType(MapKey::string(), MapValue::string()),
                new ListType(ListElement::integer()),
            ],
            false,
        ];

        yield 'list of unique same structures' => [
            [
                type_int(),
            ],
            [
                new StructureType(
                    new StructureElement('id', type_int()),
                    new StructureElement('name', type_string())
                ),
            ],
            false,
        ];

        yield 'map with string key, of maps string with string' => [
            [
                type_string(),
            ],
            [
                new MapType(
                    MapKey::string(),
                    MapValue::map(
                        new MapType(MapKey::string(), MapValue::string()),
                    )
                ),
            ],
            true,
        ];

        yield 'array of nulls' => [
            [
                type_string(),
            ],
            [
                type_null(),
                type_null(),
                type_null(),
            ],
            false,
        ];
    }

    public static function provide_structure_data() : \Generator
    {
        yield 'simple list' => [
            [
                type_int(),
            ],
            [
                type_string(),
            ],
            false,
        ];

        yield 'simple map' => [
            [
                type_string(),
            ],
            [
                type_string(),
            ],
            false,
        ];

        yield 'simple structure' => [
            [
                type_string(),
            ],
            [
                type_string(),
                new MapType(MapKey::string(), MapValue::string()),
                new ListType(ListElement::integer()),
            ],
            true,
        ];

        yield 'list of unique same structures' => [
            [
                type_int(),
            ],
            [
                new StructureType(
                    new StructureElement('id', type_int()),
                    new StructureElement('name', type_string())
                ),
            ],
            false,
        ];

        yield 'map with string key, of maps string with string' => [
            [
                type_string(),
            ],
            [
                new MapType(
                    MapKey::string(),
                    MapValue::map(
                        new MapType(MapKey::string(), MapValue::string()),
                    )
                ),
            ],
            false,
        ];

        yield 'array of nulls' => [
            [
                type_string(),
            ],
            [
                type_null(),
                type_null(),
                type_null(),
            ],
            false,
        ];

        yield 'array of empty arrays' => [
            [
                type_string(),
            ],
            [
                ArrayType::empty(),
                ArrayType::empty(),
                ArrayType::empty(),
            ],
            false,
        ];
    }

    #[DataProvider('provide_list_data')]
    public function test_list_data(array $keys, array $values, bool $expected) : void
    {
        $this->assertSame(
            $expected,
            (new ArrayContentDetector(new Types(...$keys), new Types(...$values)))->isList()
        );
    }

    #[DataProvider('provide_map_data')]
    public function test_map_data(array $keys, array $values, bool $expected) : void
    {
        $this->assertSame(
            $expected,
            (new ArrayContentDetector(new Types(...$keys), new Types(...$values)))->isMap()
        );
    }

    #[DataProvider('provide_structure_data')]
    public function test_structure_data(array $keys, array $values, bool $expected) : void
    {
        $this->assertSame(
            $expected,
            (new ArrayContentDetector(new Types(...$keys), new Types(...$values)))->isStructure()
        );
    }
}
