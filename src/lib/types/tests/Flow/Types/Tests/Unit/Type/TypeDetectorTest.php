<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type;

use DateInterval;
use DateTime;
use DateTimeZone;
use Dom\HTMLDocument;
use DOMDocument;
use Flow\ETL\Tests\Fixtures\Enum\BasicEnum;
use Flow\Types\Type;
use Flow\Types\Type\Logical\DateTimeType;
use Flow\Types\Type\Logical\DateType;
use Flow\Types\Type\Logical\HTMLType;
use Flow\Types\Type\Logical\InstanceOfType;
use Flow\Types\Type\Logical\JsonType;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\StructureType;
use Flow\Types\Type\Logical\TimeType;
use Flow\Types\Type\Logical\TimeZoneType;
use Flow\Types\Type\Logical\UuidType;
use Flow\Types\Type\Logical\XMLElementType;
use Flow\Types\Type\Logical\XMLType;
use Flow\Types\Type\Native\ArrayType;
use Flow\Types\Type\Native\EnumType;
use Flow\Types\Type\Native\NullType;
use Flow\Types\Type\Native\StringType;
use Flow\Types\Type\TypeDetector;
use Flow\Types\Value\Json;
use Flow\Types\Value\Uuid;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\RequiresPhp;
use PHPUnit\Framework\TestCase;
use stdClass;

use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;

final class TypeDetectorTest extends TestCase
{
    public static function provide_logical_types_data(): Generator
    {
        yield 'null' => [
            null,
            NullType::class,
            'null',
        ];

        yield 'json' => [
            new Json('{"one": "one", "two": "two", "three": "three"}'),
            JsonType::class,
            'json',
        ];

        yield 'time' => [
            new DateInterval('PT1H'),
            TimeType::class,
            'time',
        ];

        yield 'timezone' => [
            new DateTimeZone('UTC'),
            TimeZoneType::class,
            'timezone',
        ];

        yield 'timezone_america' => [
            new DateTimeZone('America/New_York'),
            TimeZoneType::class,
            'timezone',
        ];

        yield 'date' => [
            new DateTime('2024-01-01'),
            DateType::class,
            'date',
        ];

        yield 'datetime' => [
            new DateTime(),
            DateTimeType::class,
            'datetime',
        ];

        yield 'uuid_string' => [
            'f6d6e0e8-4b7e-4b0e-8d7a-ff0a0c9c9a5a',
            StringType::class,
            'string',
        ];

        yield 'uuid' => [
            Uuid::fromString('f6d6e0e8-4b7e-4b0e-8d7a-ff0a0c9c9a5a'),
            UuidType::class,
            'uuid',
        ];

        $xml = new DOMDocument();
        $xml->loadXML('<xml><items><item>1</item></items></xml>');
        yield 'xml' => [
            $xml,
            XMLType::class,
            'xml',
        ];

        yield 'xml_element' => [
            $xml->documentElement,
            XMLElementType::class,
            'xml_element',
        ];

        yield 'html string' => [
            '<!DOCTYPE html><html lang="en"><head></head><body><div><span>1</span></div></body></html>',
            StringType::class,
            'string',
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
            MapType::class,
            'map<string, string>',
        ];

        yield 'simple structure' => [
            [
                'one' => 'one',
                'two' => 'two',
                'three' => 'three',
                'list' => [
                    1,
                    2,
                    3,
                ],
                'map' => [
                    'one' => 'one',
                    'two' => 'two',
                    'three' => 'three',
                ],
            ],
            StructureType::class,
            'structure{one: string, two: string, three: string, list: list<integer>, map: map<string, string>}',
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

        yield 'nullable list of integers' => [
            [
                null,
                1,
                2,
                null,
                3,
            ],
            ListType::class,
            'list<?integer>',
        ];

        yield 'nullable map of string to int' => [
            [
                'one' => null,
                'two' => null,
                'three' => 3,
            ],
            MapType::class,
            'map<string, ?integer>',
        ];

        yield 'structure with first null element and then mixed int and string' => [
            [
                'one' => null,
                'two' => null,
                'three' => 3,
                'four' => '4',
            ],
            StructureType::class,
            'structure{one: null, two: null, three: integer, four: string}',
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
            MapType::class,
            'map<string, map<string, map<string, string>>>',
        ];

        yield 'empty array' => [
            [],
            ArrayType::class,
            'array<mixed>',
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
            'list<?integer>',
        ];

        yield 'map of int to string' => [
            [
                10 => '10',
                20 => '20',
                30 => '30',
            ],
            MapType::class,
            'map<integer, string>',
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
                    1,
                    2,
                    3,
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
                        1,
                        2,
                        3,
                    ],
                    'map' => [
                        'one' => 'one',
                        'two' => 'two',
                        'three' => 'three',
                    ],
                ],
                'list' => [
                    1,
                    2,
                    3,
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
                    1,
                    2,
                    3,
                ],
                [
                    4,
                    5,
                    6,
                ],
            ],
            ListType::class,
            'list<list<integer>>',
        ];

        yield 'list of floats' => [
            [
                [
                    1.2,
                    2.15414,
                    3.13,
                ],
                [
                    4.0,
                    5,
                    6,
                ],
            ],
            ListType::class,
            'list<list<float>>',
        ];

        yield 'list of lists with null' => [
            [
                [
                    1,
                    2,
                    3,
                ],
                null,
                [
                    4,
                    5,
                    6,
                ],
            ],
            ListType::class,
            'list<?list<integer>>',
        ];

        yield 'list of lists with empty' => [
            [
                [
                    1,
                    2,
                    3,
                ],
                [],
                [
                    4,
                    5,
                    6,
                ],
            ],
            ListType::class,
            'list<list<integer>>',
        ];

        yield 'list of lists with array of nulls' => [
            [
                [
                    1,
                    2,
                    3,
                ],
                [
                    null,
                ],
                [
                    4,
                    5,
                    6,
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
            MapType::class,
            'map<string, ?string>',
        ];
    }

    public static function provide_object_data(): Generator
    {
        yield 'stdclass' => [
            new stdClass(),
        ];
    }

    public static function provide_scalar_data(): Generator
    {
        yield 'bool' => [
            true,
            'boolean',
            type_boolean(),
        ];

        yield 'string' => [
            'test',
            'string',
            type_string(),
        ];

        yield 'float' => [
            1.666,
            'float',
            type_float(),
        ];

        yield 'integer' => [
            123456789,
            'integer',
            type_integer(),
        ];
    }

    public function test_enum_type(): void
    {
        static::assertInstanceOf(EnumType::class, (new TypeDetector())->detectType(BasicEnum::two));
    }

    #[RequiresPhp('>= 8.4')]
    public function test_logical_html_element_type(): void
    {
        // @mago-expect analysis:unavailable-method
        $document = HTMLDocument::createFromString(
            '<!DOCTYPE html><html lang="en"><head></head><body><div><span>1</span></div></body></html>',
        );

        $type = (new TypeDetector())->detectType($document->querySelector('body'));

        static::assertInstanceOf(Type\Logical\HTMLElementType::class, $type);
        static::assertSame('html_element', $type->toString());
    }

    #[RequiresPhp('>= 8.4')]
    public function test_logical_html_type(): void
    {
        // @mago-expect analysis:unavailable-method
        $type = (new TypeDetector())->detectType(HTMLDocument::createFromString(
            '<!DOCTYPE html><html lang="en"><head></head><body><div><span>1</span></div></body></html>',
        ));

        static::assertInstanceOf(HTMLType::class, $type);
        static::assertSame('html', $type->toString());
    }

    /**
     * @param class-string $class
     */
    #[DataProvider('provide_logical_types_data')]
    public function test_logical_types(mixed $data, string $class, string $description): void
    {
        $type = (new TypeDetector())->detectType($data);

        static::assertInstanceOf($class, $type);
        static::assertSame($description, $type->toString());
    }

    #[DataProvider('provide_object_data')]
    public function test_object_types(mixed $data): void
    {
        static::assertInstanceOf(InstanceOfType::class, (new TypeDetector())->detectType($data));
    }

    /**
     * @param Type<mixed> $expectedType
     */
    #[DataProvider('provide_scalar_data')]
    public function test_scalar_types(mixed $data, string $description, Type $expectedType): void
    {
        $type = (new TypeDetector())->detectType($data);
        static::assertInstanceOf($expectedType::class, $type);
        static::assertSame($description, $type->toString());
    }
}
