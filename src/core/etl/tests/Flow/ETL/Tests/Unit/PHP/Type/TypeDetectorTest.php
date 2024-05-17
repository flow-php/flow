<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type;

use Flow\ETL\PHP\Type\Logical\{DateTimeType,
    JsonType,
    ListType,
    StructureType,
    UuidType,
    XMLElementType,
    XMLType};
use Flow\ETL\PHP\Type\Native\{ArrayType, EnumType, NullType, ObjectType, ScalarType};
use Flow\ETL\PHP\Type\TypeDetector;
use Flow\ETL\PHP\Value\Uuid;
use Flow\ETL\Tests\Fixtures\Enum\BasicEnum;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class TypeDetectorTest extends TestCase
{
    public static function provide_logical_types_data() : \Generator
    {
        yield 'null' => [
            null,
            NullType::class,
            'null',
        ];

        yield 'json' => [
            '{"one": "one", "two": "two", "three": "three"}',
            JsonType::class,
            'json',
        ];

        yield 'datetime' => [
            new \DateTime(),
            DateTimeType::class,
            'datetime',
        ];

        yield 'uuid_string' => [
            'f6d6e0e8-4b7e-4b0e-8d7a-ff0a0c9c9a5a',
            ScalarType::class,
            'string',
        ];

        yield 'uuid' => [
            Uuid::fromString('f6d6e0e8-4b7e-4b0e-8d7a-ff0a0c9c9a5a'),
            UuidType::class,
            'uuid',
        ];

        $dom = new \DOMDocument();
        $dom->loadXML('<xml><items><item>1</item></items></xml>');
        yield 'xml' => [
            $dom,
            XMLType::class,
            'xml',
        ];

        yield 'xml_element' => [
            $dom->documentElement,
            XMLElementType::class,
            'xml_element',
        ];

        yield 'simple list' => [
            [
                'one',
                'two',
                'three',
            ],
            ListType::class,
            'list<string>',
        ];

        yield 'simple map' => [
            [
                'one' => 'one',
                'two' => 'two',
                'three' => 'three',
            ],
            StructureType::class,
            'structure{one: string, two: string, three: string}',
        ];

        yield 'simple structure' => [
            [
                'one' => 'one',
                'two' => 'two',
                'three' => 'three',
                'list' => [
                    1, 2, 3,
                ],
                'map' => [
                    'one' => 'one',
                    'two' => 'two',
                    'three' => 'three',
                ],
            ],
            StructureType::class,
            'structure{one: string, two: string, three: string, list: list<integer>, map: structure{one: string, two: string, three: string}}',
        ];

        yield 'list of unique same structures' => [
            [
                [
                    'id' => 1,
                    'name' => 'Test 1',
                ],
                [
                    'id' => 2,
                    'name' => 'Test 2',
                ],
            ],
            ListType::class,
            'list<structure{id: integer, name: string}>',
        ];

        yield 'map with string key, of maps string with string' => [
            [
                'one' => [
                    'map' => [
                        'one' => 'one',
                        'two' => 'two',
                        'three' => 'three',
                    ],
                ],
                'two' => [
                    'map' => [
                        'one' => 'one',
                        'two' => 'two',
                        'three' => 'three',
                    ],
                ],
            ],
            StructureType::class,
            'structure{one: structure{map: structure{one: string, two: string, three: string}}, two: structure{map: structure{one: string, two: string, three: string}}}',
        ];

        yield 'empty array' => [
            [],
            ArrayType::class,
            'array<empty, empty>',
        ];

        yield 'list with null' => [
            [
                1,
                2,
                3,
                null,
                5,
            ],
            ListType::class,
            'list<integer>',
        ];

        yield 'one level list' => [
            [
                'one',
                'two',
                'three',
                'map' => [
                    'one' => 'one',
                    'two' => 'two',
                    'three' => 'three',
                ],
                'list' => [
                    1, 2, 3,
                ],
            ],
            ArrayType::class,
            'array<mixed>',
        ];

        yield 'two level list' => [
            [
                'one',
                'two',
                'three',
                'map' => [
                    'one' => 'one',
                    'two' => 'two',
                    'three' => 'three',
                    'list' => [
                        1, 2, 3,
                    ],
                    'map' => [
                        'one' => 'one',
                        'two' => 'two',
                        'three' => 'three',
                    ],
                ],
                'list' => [
                    1, 2, 3,
                ],
            ],
            ArrayType::class,
            'array<mixed>',
        ];

        yield 'complex structure' => [
            [
                [
                    'id' => 1,
                    'name' => 'Test 1',
                    'active' => true,
                ],
                [
                    'id' => 2,
                    'name' => 'Test 2',
                ],
            ],
            ArrayType::class,
            'array<mixed>',
        ];

        yield 'list of lists' => [
            [
                [
                    1, 2, 3,
                ],
                [
                    4, 5, 6,
                ],
            ],
            ListType::class,
            'list<list<integer>>',
        ];

        yield 'list of lists with null' => [
            [
                [
                    1, 2, 3,
                ],
                null,
                [
                    4, 5, 6,
                ],
            ],
            ListType::class,
            'list<list<integer>>',
        ];

        yield 'list of lists with empty' => [
            [
                [
                    1, 2, 3,
                ],
                [
                ],
                [
                    4, 5, 6,
                ],
            ],
            ListType::class,
            'list<list<integer>>',
        ];

        yield 'list of lists with array of nulls' => [
            [
                [
                    1, 2, 3,
                ],
                [
                    null,
                ],
                [
                    4, 5, 6,
                ],
            ],
            ListType::class,
            'list<list<integer>>',
        ];

        yield 'map with null' => [
            [
                'one' => 'one',
                'two' => null,
                'three' => 'three',
            ],
            StructureType::class,
            'structure{one: string, two: null, three: string}',
        ];
    }

    public static function provide_object_data() : \Generator
    {
        yield 'stdclass' => [
            new \stdClass(),
        ];
    }

    public static function provide_scalar_data() : \Generator
    {
        yield 'bool' => [
            true,
            'boolean',
        ];

        yield 'string' => [
            'test',
            'string',
        ];

        yield 'float' => [
            1.666,
            'float',
        ];

        yield 'integer' => [
            123456789,
            'integer',
        ];
    }

    public function test_enum_type() : void
    {
        self::assertInstanceOf(EnumType::class, (new TypeDetector())->detectType(BasicEnum::two));
    }

    #[DataProvider('provide_logical_types_data')]
    public function test_logical_types($data, string $class, string $description) : void
    {
        $type = (new TypeDetector())->detectType($data);

        self::assertInstanceOf($class, $type);
        self::assertSame($description, $type->toString());
    }

    #[DataProvider('provide_object_data')]
    public function test_object_types(mixed $data) : void
    {
        self::assertInstanceOf(ObjectType::class, (new TypeDetector())->detectType($data));
    }

    #[DataProvider('provide_scalar_data')]
    public function test_scalar_types(mixed $data, string $description) : void
    {
        $type = (new TypeDetector())->detectType($data);
        self::assertInstanceOf(ScalarType::class, $type);
        self::assertSame($description, $type->toString());
    }
}
