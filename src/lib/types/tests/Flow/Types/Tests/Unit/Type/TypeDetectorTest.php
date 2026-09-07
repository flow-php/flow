<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type;

use DateInterval;
use DateTime;
use DateTimeZone;
use Dom\HTMLDocument;
use DOMDocument;
use Flow\ETL\Tests\Fixtures\Enum\BasicEnum;
use Flow\Types\Exception\InvalidArgumentException;
use Flow\Types\Tests\Double\InvokableObject;
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
use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;

final class TypeDetectorTest extends TestCase
{
    public static function provide_detected_type_law_data(): Generator
    {
        yield 'empty array' => [[]];

        yield 'homogeneous list' => [[1, 2, 3]];

        yield 'list of int and float' => [[1, 1.5]];

        yield 'list of int and string' => [[1, 'a']];

        yield 'list of nulls and integers' => [[null, 1]];

        yield 'lists of int and float' => [[[1], [1.5]]];

        yield 'structures with conflicting element types' => [[['a' => 1], ['a' => 'x']]];

        yield 'depth three with a conflicting leaf' => [[[['a' => 1], ['a' => 1.5]]]];

        yield 'depth three homogeneous' => [[[['a' => 1], ['a' => 2]]]];

        yield 'map of int to conflicting values' => [[1 => 'a', 2 => 3]];

        yield 'structure holding an empty array' => [['a' => [], 'b' => [1]]];

        yield 'list mixing scalars and arrays' => [[1, [2]]];

        yield 'nested empty arrays' => [[[], []]];

        yield 'string keyed map with mixed nesting' => [['a' => ['b' => 1], 'c' => ['b' => 'x']]];
    }

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

        yield 'homogeneous string keys are structural, not a map' => [
            [
                'one' => 'one',
                'two' => 'two',
                'three' => 'three',
            ],
            StructureType::class,
            'structure{one: string, two: string, three: string}',
        ];

        yield 'integer keys, non list' => [
            [
                1 => 'one',
                2 => 'two',
            ],
            MapType::class,
            'map<integer, string>',
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
                'nested' => [
                    'one' => 'one',
                    'two' => 'two',
                    'three' => 'three',
                ],
            ],
            StructureType::class,
            'structure{one: string, two: string, three: string, list: list<integer>, nested: structure{one: string, two: string, three: string}}',
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

        yield 'string keys with leading nulls' => [
            [
                'one' => null,
                'two' => null,
                'three' => 3,
            ],
            StructureType::class,
            'structure{one: null, two: null, three: integer}',
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

        yield 'string keys, nested all the way down' => [
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
            ListType::class,
            'list<null>',
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
            ListType::class,
            'list<structure{id: integer, name: string, active?: boolean}>',
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
            // [4.0, 5, 6] mixes float and integer; the element types unify to float.
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
            // The empty element observes no value, so the element type is not known to be present.
            'list<list<?integer>>',
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
            'list<array<mixed>>',
        ];

        yield 'string keys with an interleaved null' => [
            [
                'one' => 'one',
                'two' => null,
                'three' => 'three',
            ],
            StructureType::class,
            'structure{one: string, two: null, three: string}',
        ];

        yield 'string keys, all null' => [
            [
                'one' => null,
                'two' => null,
            ],
            StructureType::class,
            'structure{one: null, two: null}',
        ];

        yield 'heterogeneous list of int and string' => [
            [1, 'a'],
            ArrayType::class,
            'array<mixed>',
        ];

        yield 'list of int and float promotes to float' => [
            [1, 1.5],
            ListType::class,
            'list<float>',
        ];

        yield 'map of int to int and float promotes to float' => [
            [1 => 1, 5 => 2.5],
            MapType::class,
            'map<integer, float>',
        ];

        yield 'map of int to structures differing by one field' => [
            [1 => ['a' => 1], 5 => ['a' => 1, 'b' => 2]],
            MapType::class,
            // the element types do not unify; the widener's optional field is what carries them
            'map<integer, structure{a: integer, b?: integer}>',
        ];

        yield 'map of int to a list and an untyped array' => [
            [1 => [1, 2], 5 => [null]],
            MapType::class,
            'map<integer, array<mixed>>',
        ];

        yield 'heterogeneous map of int to mixed' => [
            [1 => 'a', 2 => 3],
            ArrayType::class,
            'array<mixed>',
        ];

        yield 'structure with mixed value types' => [
            ['a' => 1, 'b' => 'x'],
            StructureType::class,
            'structure{a: integer, b: string}',
        ];

        yield 'mixed int and string keys' => [
            [0 => 'a', 'x' => 'b'],
            ArrayType::class,
            'array<mixed>',
        ];

        yield 'heterogeneous list of array and string' => [
            [[1], 'a'],
            ArrayType::class,
            'array<mixed>',
        ];

        yield 'list with only an empty array' => [
            [[]],
            ListType::class,
            'list<list<null>>',
        ];

        yield 'list with only empty arrays' => [
            [[], []],
            ListType::class,
            'list<list<null>>',
        ];

        yield 'empty array before a list' => [
            [[], [1, 2]],
            ListType::class,
            'list<list<?integer>>',
        ];

        yield 'empty array after a list' => [
            [[1, 2], []],
            ListType::class,
            'list<list<?integer>>',
        ];

        yield 'empty array before a structure' => [
            [[], ['id' => '1']],
            ArrayType::class,
            'array<mixed>',
        ];

        yield 'empty array after a structure' => [
            [['id' => '1'], []],
            ArrayType::class,
            'array<mixed>',
        ];

        yield 'structure with an empty array element' => [
            ['data' => []],
            StructureType::class,
            'structure{data: list<null>}',
        ];

        yield 'structure with an empty array element and a scalar' => [
            ['data' => [], 'x' => 1],
            StructureType::class,
            'structure{data: list<null>, x: integer}',
        ];

        yield 'non-list integer keys with empty array values' => [
            [1 => [], 2 => []],
            MapType::class,
            'map<integer, list<null>>',
        ];

        yield 'null before an empty array' => [
            [null, []],
            ListType::class,
            'list<?list<null>>',
        ];

        yield 'string with an empty array' => [
            ['a', []],
            ArrayType::class,
            'array<mixed>',
        ];

        yield 'empty array with a string' => [
            [[], 'a'],
            ArrayType::class,
            'array<mixed>',
        ];

        yield 'string with an empty array and a null' => [
            ['a', [], null],
            ArrayType::class,
            'array<mixed>',
        ];

        yield 'non-list integer keys with a string and an empty array' => [
            [5 => 'a', 6 => []],
            ArrayType::class,
            'array<mixed>',
        ];

        yield 'non-list integer keys with a string and a mixed-keys array' => [
            [5 => 'a', 6 => [1 => 'x', 'y' => 2]],
            ArrayType::class,
            'array<mixed>',
        ];

        yield 'null after an empty array' => [
            [[], null],
            ListType::class,
            'list<?list<null>>',
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

    public function test_closure_is_rejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Closure is not a supported value type.');

        (new TypeDetector())->detectType(static fn(): int => 1);
    }

    #[DataProvider('provide_detected_type_law_data')]
    public function test_detection_is_idempotent_under_its_own_materialization(mixed $data): void
    {
        $type = (new TypeDetector())->detectType($data);

        static::assertSame(
            $type->toString(),
            (new TypeDetector())
                ->detectType($type->cast($data))
                ->toString(),
        );
    }

    public function test_enum_type(): void
    {
        static::assertInstanceOf(EnumType::class, (new TypeDetector())->detectType(BasicEnum::two));
    }

    public function test_invokable_object_is_still_detected_as_instance_of(): void
    {
        static::assertEquals(
            type_instance_of(InvokableObject::class),
            (new TypeDetector())->detectType(new InvokableObject()),
        );
    }

    #[RequiresPhp('>= 8.4.0')]
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

    #[RequiresPhp('>= 8.4.0')]
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
