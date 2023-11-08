<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\ArrayContentDetector;
use Flow\ETL\PHP\Type\Logical\List\ListElement;
use Flow\ETL\PHP\Type\Logical\ListType;
use Flow\ETL\PHP\Type\Logical\Map\MapKey;
use Flow\ETL\PHP\Type\Logical\Map\MapValue;
use Flow\ETL\PHP\Type\Logical\MapType;
use Flow\ETL\PHP\Type\Logical\Structure\StructureElement;
use Flow\ETL\PHP\Type\Logical\StructureType;
use Flow\ETL\PHP\Type\Native\ScalarType;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class ArrayContentDetectorTest extends TestCase
{
    public static function provide_list_data() : \Generator
    {
        yield 'simple list' => [
            [
                ScalarType::integer(),
            ],
            [
                ScalarType::string(),
            ],
            true,
        ];

        yield 'simple map' => [
            [
                ScalarType::string(),
            ],
            [
                ScalarType::string(),
            ],
            false,
        ];

        yield 'simple structure' => [
            [
                ScalarType::string(),
            ],
            [
                ScalarType::string(),
                new MapType(MapKey::string(), MapValue::string()),
                new ListType(ListElement::integer()),
            ],
            false,
        ];

        yield 'list of unique same structures' => [
            [
                ScalarType::integer(),
            ],
            [
                new StructureType(
                    new StructureElement('id', ScalarType::integer()),
                    new StructureElement('name', ScalarType::string())
                ),
            ],
            true,
        ];

        yield 'map with string key, of maps string with string' => [
            [
                ScalarType::string(),
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
    }

    public static function provide_map_data() : \Generator
    {
        yield 'simple list' => [
            [
                ScalarType::integer(),
            ],
            [
                ScalarType::string(),
            ],
            false,
        ];

        yield 'simple map' => [
            [
                ScalarType::string(),
            ],
            [
                ScalarType::string(),
            ],
            true,
        ];

        yield 'simple structure' => [
            [
                ScalarType::string(),
            ],
            [
                ScalarType::string(),
                new MapType(MapKey::string(), MapValue::string()),
                new ListType(ListElement::integer()),
            ],
            false,
        ];

        yield 'list of unique same structures' => [
            [
                ScalarType::integer(),
            ],
            [
                new StructureType(
                    new StructureElement('id', ScalarType::integer()),
                    new StructureElement('name', ScalarType::string())
                ),
            ],
            false,
        ];

        yield 'map with string key, of maps string with string' => [
            [
                ScalarType::string(),
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
    }

    public static function provide_structure_data() : \Generator
    {
        yield 'simple list' => [
            [
                ScalarType::integer(),
            ],
            [
                ScalarType::string(),
            ],
            false,
        ];

        yield 'simple map' => [
            [
                ScalarType::string(),
            ],
            [
                ScalarType::string(),
            ],
            false,
        ];

        yield 'simple structure' => [
            [
                ScalarType::string(),
            ],
            [
                ScalarType::string(),
                new MapType(MapKey::string(), MapValue::string()),
                new ListType(ListElement::integer()),
            ],
            true,
        ];

        yield 'list of unique same structures' => [
            [
                ScalarType::integer(),
            ],
            [
                new StructureType(
                    new StructureElement('id', ScalarType::integer()),
                    new StructureElement('name', ScalarType::string())
                ),
            ],
            false,
        ];

        yield 'map with string key, of maps string with string' => [
            [
                ScalarType::string(),
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
    }

    public function test_empty_keys() : void
    {
        $this->expectExceptionMessage('Key type list cannot be empty');
        $this->expectException(InvalidArgumentException::class);

        (new ArrayContentDetector([], []));
    }

    public function test_empty_values() : void
    {
        $this->expectExceptionMessage('Value type list cannot be empty');
        $this->expectException(InvalidArgumentException::class);

        (new ArrayContentDetector([1], []));
    }

    #[DataProvider('provide_list_data')]
    public function test_list_data(array $keys, array $values, bool $expected) : void
    {
        $this->assertSame(
            $expected,
            (new ArrayContentDetector($keys, $values))->isList()
        );
    }

    #[DataProvider('provide_map_data')]
    public function test_map_data(array $keys, array $values, bool $expected) : void
    {
        $this->assertSame(
            $expected,
            (new ArrayContentDetector($keys, $values))->isMap()
        );
    }

    #[DataProvider('provide_structure_data')]
    public function test_structure_data(array $keys, array $values, bool $expected) : void
    {
        $this->assertSame(
            $expected,
            (new ArrayContentDetector($keys, $values))->isStructure()
        );
    }
}
