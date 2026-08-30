<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Logical;

use DateTimeZone;
use Flow\Types\Exception\CastingException;
use Flow\Types\Exception\InvalidArgumentException;
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Exception\MissingElementCastingException;
use Flow\Types\Type;
use Flow\Types\Type\Logical\StructureElement;
use Flow\Types\Type\Logical\StructureType;
use Generator;
use JsonException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use stdClass;

use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_from_array;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_mixed;
use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_union;

final class StructureTypeTest extends TestCase
{
    public static function assert_data_provider(): Generator
    {
        yield 'valid structure with required fields' => [
            'value' => ['id' => 1, 'name' => 'b'],
            'structureType' => type_structure(['id' => type_integer(), 'name' => type_string()]),
            'exceptionClass' => null,
        ];

        yield 'valid structure with optional field' => [
            'value' => ['id' => 1, 'name' => null],
            'structureType' => type_structure(['id' => type_integer(), 'name' => type_optional(type_string())]),
            'exceptionClass' => null,
        ];

        yield 'invalid string' => [
            'value' => 'string',
            'structureType' => type_structure(['id' => type_integer(), 'name' => type_string()]),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid UUID string' => [
            'value' => '49e952c8-80ec-4910-a1d6-a19bd46b163d',
            'structureType' => type_structure(['id' => type_integer(), 'name' => type_string()]),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid boolean' => [
            'value' => false,
            'structureType' => type_structure(['id' => type_integer(), 'name' => type_string()]),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid float' => [
            'value' => 124.25,
            'structureType' => type_structure(['id' => type_integer(), 'name' => type_string()]),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid array with different keys' => [
            'value' => ['a' => 'a', 'b' => 'b'],
            'structureType' => type_structure(['id' => type_integer(), 'name' => type_string()]),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid object' => [
            'value' => new stdClass(),
            'structureType' => type_structure(['id' => type_integer(), 'name' => type_string()]),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid DateTimeZone' => [
            'value' => new DateTimeZone('UTC'),
            'structureType' => type_structure(['id' => type_integer(), 'name' => type_string()]),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid structure with null required field' => [
            'value' => ['id' => null, 'name' => 'b'],
            'structureType' => type_structure(['id' => type_integer(), 'name' => type_string()]),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid structure with null required field 2' => [
            'value' => ['id' => 2, 'name' => null],
            'structureType' => type_structure(['id' => type_integer(), 'name' => type_string()]),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid structure with all null fields' => [
            'value' => ['id' => null, 'name' => null],
            'structureType' => type_structure(['id' => type_integer(), 'name' => type_string()]),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid structure with extra field' => [
            'value' => ['id' => 1, 'name' => null, 'active' => false],
            'structureType' => type_structure(['id' => type_integer(), 'name' => type_string()]),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'valid structure with extra field when allow_extra is true' => [
            'value' => ['id' => 1, 'name' => 'test', 'active' => false],
            'structureType' => type_structure(['id' => type_integer(), 'name' => type_string()], true),
            'exceptionClass' => null,
        ];

        yield 'valid structure with type_array field containing a list of structures' => [
            'value' => [
                'id' => 'test-id',
                'size' => 123,
                'schema' => [
                    ['ref' => 'col1', 'type' => ['key' => 'value'], 'metadata' => [], 'nullable' => true],
                    ['ref' => 'col2', 'type' => ['key2' => 'value2'], 'metadata' => [], 'nullable' => false],
                ],
                'rows_count' => 10,
                'processed_rows' => 5,
                'synchronization_id' => 'sync-123',
            ],
            'structureType' => type_structure([
                'id' => type_string(),
                'size' => type_integer(),
                'schema' => type_array(),
                'rows_count' => type_integer(),
                'processed_rows' => type_integer(),
                'synchronization_id' => type_string(),
            ]),
            'exceptionClass' => null,
        ];

        yield 'valid structure with type_list(type_mixed()) field containing a list of structures' => [
            'value' => [
                'id' => 'test-id',
                'size' => 123,
                'schema' => [
                    ['ref' => 'col1', 'type' => ['key' => 'value'], 'metadata' => [], 'nullable' => true],
                    ['ref' => 'col2', 'type' => ['key2' => 'value2'], 'metadata' => [], 'nullable' => false],
                ],
                'rows_count' => 10,
                'processed_rows' => 5,
                'synchronization_id' => 'sync-123',
            ],
            'structureType' => type_structure([
                'id' => type_string(),
                'size' => type_integer(),
                'schema' => type_list(type_mixed()),
                'rows_count' => type_integer(),
                'processed_rows' => type_integer(),
                'synchronization_id' => type_string(),
            ]),
            'exceptionClass' => null,
        ];

        yield 'valid structure with multiple extra fields when allow_extra is true' => [
            'value' => ['id' => 1, 'name' => 'test', 'active' => false, 'created_at' => '2023-01-01'],
            'structureType' => type_structure(['id' => type_integer(), 'name' => type_string()], true),
            'exceptionClass' => null,
        ];

        yield 'invalid structure with missing required field even when allow_extra is true' => [
            'value' => ['name' => 'test', 'active' => false],
            'structureType' => type_structure(['id' => type_integer(), 'name' => type_string()], true),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'valid structure with optional elements present' => [
            'value' => ['id' => 1, 'name' => 'test', 'active' => false],
            'structureType' => type_structure([
                'id' => type_integer(),
                'name' => type_string(),
                'active' => structure_element('active', type_boolean(), optional: true),
            ]),
            'exceptionClass' => null,
        ];

        yield 'valid structure with optional elements missing' => [
            'value' => ['id' => 1, 'name' => 'test'],
            'structureType' => type_structure([
                'id' => type_integer(),
                'name' => type_string(),
                'active' => structure_element('active', type_boolean(), optional: true),
            ]),
            'exceptionClass' => null,
        ];

        yield 'valid structure with some optional elements present' => [
            'value' => ['id' => 1, 'name' => 'test', 'active' => false],
            'structureType' => type_structure([
                'id' => type_integer(),
                'name' => type_string(),
                'active' => structure_element('active', type_boolean(), optional: true),
                'created_at' => structure_element('created_at', type_string(), optional: true),
            ]),
            'exceptionClass' => null,
        ];

        yield 'invalid structure with wrong type for optional element' => [
            'value' => ['id' => 1, 'name' => 'test', 'active' => 'invalid'],
            'structureType' => type_structure([
                'id' => type_integer(),
                'name' => type_string(),
                'active' => structure_element('active', type_boolean(), optional: true),
            ]),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid structure with unknown field when optional elements present and allow_extra false' => [
            'value' => ['id' => 1, 'name' => 'test', 'unknown' => 'value'],
            'structureType' => type_structure([
                'id' => type_integer(),
                'name' => type_string(),
                'active' => structure_element('active', type_boolean(), optional: true),
            ]),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'valid structure with unknown field when optional elements present and allow_extra true' => [
            'value' => ['id' => 1, 'name' => 'test', 'unknown' => 'value'],
            'structureType' => type_structure([
                'id' => type_integer(),
                'name' => type_string(),
                'active' => structure_element('active', type_boolean(), optional: true),
            ], true),
            'exceptionClass' => null,
        ];
    }

    public static function cast_data_provider(): Generator
    {
        yield 'array into structure' => [
            'structure' => type_structure([
                'name' => type_string(),
                'age' => type_integer(),
                'address' => type_structure([
                    'street' => type_string(),
                    'city' => type_string(),
                ]),
            ]),
            'value' => [
                'name' => 'Norbert Orzechowicz',
                'age' => 30,
                'address' => [
                    'street' => 'Polna',
                    'city' => 'Warsaw',
                ],
            ],
            'expected' => [
                'name' => 'Norbert Orzechowicz',
                'age' => 30,
                'address' => [
                    'street' => 'Polna',
                    'city' => 'Warsaw',
                ],
            ],
            'exceptionClass' => null,
        ];

        yield 'structure with empty not nullable fields' => [
            'structure' => type_structure([
                'name' => type_string(),
                'age' => type_integer(),
                'address' => type_structure([
                    'street' => type_optional(type_string()),
                    'city' => type_optional(type_string()),
                ]),
            ]),
            'value' => [
                'name' => 'Norbert Orzechowicz',
                'age' => 30,
                'address' => [],
            ],
            'expected' => [
                'name' => 'Norbert Orzechowicz',
                'age' => 30,
                'address' => [
                    'street' => null,
                    'city' => null,
                ],
            ],
            'exceptionClass' => null,
        ];

        yield 'structure with missing nullable fields' => [
            'structure' => type_structure([
                'name' => type_string(),
                'age' => type_integer(),
                'address' => type_optional(type_structure([
                    'street' => type_string(),
                    'city' => type_string(),
                ])),
            ]),
            'value' => [
                'name' => 'Norbert Orzechowicz',
                'age' => 30,
            ],
            'expected' => [
                'name' => 'Norbert Orzechowicz',
                'age' => 30,
                'address' => null,
            ],
            'exceptionClass' => null,
        ];

        yield 'throws on missing required element' => [
            'structure' => type_structure(['id' => type_integer(), 'name' => type_string()]),
            'value' => ['id' => 1],
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];

        yield 'throws on present null required element' => [
            'structure' => type_structure(['id' => type_integer(), 'name' => type_string()]),
            'value' => ['id' => 1, 'name' => null],
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];

        yield 'throws on empty payload' => [
            'structure' => type_structure(['id' => type_integer(), 'name' => type_string()]),
            'value' => [],
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];

        yield 'throws on null payload' => [
            'structure' => type_structure(['id' => type_integer(), 'name' => type_string()]),
            'value' => null,
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];

        yield 'throws on scalar payload' => [
            'structure' => type_structure(['id' => type_integer(), 'name' => type_string()]),
            'value' => 'hello',
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];

        yield 'optional-wrapped required element stays null when present-null' => [
            'structure' => type_structure(['id' => type_integer(), 'name' => type_optional(type_string())]),
            'value' => ['id' => 1, 'name' => null],
            'expected' => ['id' => 1, 'name' => null],
            'exceptionClass' => null,
        ];

        yield 'union-with-null element stays null when present-null' => [
            'structure' => type_structure(['id' => type_integer(), 'tag' => type_union(type_string(), type_null())]),
            'value' => ['id' => 1, 'tag' => null],
            'expected' => ['id' => 1, 'tag' => null],
            'exceptionClass' => null,
        ];

        yield 'structure-level optional scalar element throws on present-null' => [
            'structure' => type_structure([
                'id' => type_integer(),
                'name' => structure_element('name', type_string(), optional: true),
            ]),
            'value' => ['id' => 1, 'name' => null],
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];

        yield 'structure-level optional list element throws on present-null' => [
            'structure' => type_structure([
                'id' => type_integer(),
                'tags' => structure_element('tags', type_list(type_string()), optional: true),
            ]),
            'value' => ['id' => 1, 'tags' => null],
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];

        yield 'absent optional element stays absent' => [
            'structure' => type_structure([
                'id' => type_integer(),
                'name' => structure_element('name', type_string(), optional: true),
            ]),
            'value' => ['id' => 1],
            'expected' => ['id' => 1],
            'exceptionClass' => null,
        ];

        yield 'present optional element is cast' => [
            'structure' => type_structure([
                'id' => type_integer(),
                'name' => structure_element('name', type_string(), optional: true),
            ]),
            'value' => ['id' => 1, 'name' => 123],
            'expected' => ['id' => 1, 'name' => '123'],
            'exceptionClass' => null,
        ];

        yield 'empty payload casts into all-optional structure' => [
            'structure' => type_structure(['data' => structure_element(
                'data',
                type_list(type_string()),
                optional: true,
            )]),
            'value' => [],
            'expected' => [],
            'exceptionClass' => null,
        ];

        yield 'empty JSON object casts into all-optional structure' => [
            'structure' => type_structure(['data' => structure_element(
                'data',
                type_list(type_string()),
                optional: true,
            )]),
            'value' => '{}',
            'expected' => [],
            'exceptionClass' => null,
        ];

        yield 'valid JSON string payload casts element-wise' => [
            'structure' => type_structure(['id' => type_integer()]),
            'value' => '{"id":"1"}',
            'expected' => ['id' => 1],
            'exceptionClass' => null,
        ];

        yield 'partial JSON string payload throws naming the element' => [
            'structure' => type_structure(['id' => type_integer(), 'name' => type_string()]),
            'value' => '{"id":1}',
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];

        yield 'malformed JSON string payload' => [
            'structure' => type_structure(['id' => type_integer(), 'name' => type_string()]),
            'value' => '{invalid',
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];
    }

    public static function is_valid_data_provider(): Generator
    {
        yield 'valid simple structure' => [
            'structure' => type_structure(['string' => type_string()]),
            'value' => ['string' => 'two'],
            'expected' => true,
        ];

        yield 'valid complex structure' => [
            'structure' => type_structure([
                'map' => type_map(type_integer(), type_map(type_string(), type_list(type_integer()))),
                'string' => type_string(),
                'float' => type_float(),
            ]),
            'value' => ['map' => [0 => ['one' => [1, 2]], 1 => ['two' => [3, 4]]], 'string' => 'c', 'float' => 1.5],
            'expected' => true,
        ];

        yield 'invalid indexed array' => [
            'structure' => type_structure(['int' => type_integer()]),
            'value' => [1, 2],
            'expected' => false,
        ];

        yield 'invalid structure with extra fields when allow_extra is false' => [
            'structure' => type_structure(['id' => type_integer(), 'name' => type_string()]),
            'value' => ['id' => 1, 'name' => 'test', 'active' => true],
            'expected' => false,
        ];

        yield 'valid structure with extra fields when allow_extra is true' => [
            'structure' => type_structure(['id' => type_integer(), 'name' => type_string()], true),
            'value' => ['id' => 1, 'name' => 'test', 'active' => true],
            'expected' => true,
        ];

        yield 'valid structure with multiple extra fields when allow_extra is true' => [
            'structure' => type_structure(['id' => type_integer(), 'name' => type_string()], true),
            'value' => [
                'id' => 1,
                'name' => 'test',
                'active' => true,
                'created_at' => '2023-01-01',
                'updated_at' => '2023-01-02',
            ],
            'expected' => true,
        ];

        yield 'invalid structure with missing required field when allow_extra is true' => [
            'structure' => type_structure(['id' => type_integer(), 'name' => type_string()], true),
            'value' => ['name' => 'test', 'active' => true],
            'expected' => false,
        ];

        yield 'valid structure with only required fields when allow_extra is true' => [
            'structure' => type_structure(['id' => type_integer(), 'name' => type_string()], true),
            'value' => ['id' => 1, 'name' => 'test'],
            'expected' => true,
        ];

        yield 'valid structure with optional elements present' => [
            'structure' => type_structure([
                'id' => type_integer(),
                'name' => structure_element('name', type_string(), optional: true),
                'active' => structure_element('active', type_boolean(), optional: true),
            ]),
            'value' => ['id' => 1, 'name' => 'test', 'active' => true],
            'expected' => true,
        ];

        yield 'valid structure with some optional elements present' => [
            'structure' => type_structure([
                'id' => type_integer(),
                'name' => structure_element('name', type_string(), optional: true),
                'active' => structure_element('active', type_boolean(), optional: true),
            ]),
            'value' => ['id' => 1, 'name' => 'test'],
            'expected' => true,
        ];

        yield 'valid structure with no optional elements present' => [
            'structure' => type_structure([
                'id' => type_integer(),
                'name' => structure_element('name', type_string(), optional: true),
                'active' => structure_element('active', type_boolean(), optional: true),
            ]),
            'value' => ['id' => 1],
            'expected' => true,
        ];

        yield 'valid empty payload for all-optional structure' => [
            'structure' => type_structure(['name' => structure_element('name', type_string(), optional: true)]),
            'value' => [],
            'expected' => true,
        ];

        yield 'invalid empty payload when required elements exist' => [
            'structure' => type_structure(['id' => type_integer()]),
            'value' => [],
            'expected' => false,
        ];

        yield 'invalid structure with wrong type for optional element' => [
            'structure' => type_structure([
                'id' => type_integer(),
                'name' => structure_element('name', type_string(), optional: true),
            ]),
            'value' => ['id' => 1, 'name' => 123],
            'expected' => false,
        ];

        yield 'invalid structure with extra field when optional elements present and allow_extra false' => [
            'structure' => type_structure([
                'id' => type_integer(),
                'name' => structure_element('name', type_string(), optional: true),
            ]),
            'value' => ['id' => 1, 'name' => 'test', 'unknown' => 'value'],
            'expected' => false,
        ];

        yield 'valid structure with extra field when optional elements present and allow_extra true' => [
            'structure' => type_structure([
                'id' => type_integer(),
                'name' => structure_element('name', type_string(), optional: true),
            ], true),
            'value' => ['id' => 1, 'name' => 'test', 'unknown' => 'value'],
            'expected' => true,
        ];
    }

    public function test_allows_extra_false_by_default(): void
    {
        $type = type_structure(['id' => type_integer()]);
        static::assertFalse($type->allowsExtra());
    }

    public function test_allows_extra_true_when_set(): void
    {
        $type = type_structure(['id' => type_integer()], true);
        static::assertTrue($type->allowsExtra());
    }

    /**
     * @param null|class-string<\Throwable> $exceptionClass
     */
    #[DataProvider('assert_data_provider')]
    public function test_assert(mixed $value, Type $structureType, ?string $exceptionClass = null): void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            $structureType->assert($value);
        } else {
            static::assertIsArray($structureType->assert($value));
        }
    }

    /**
     * @param null|class-string<\Throwable> $exceptionClass
     */
    #[DataProvider('cast_data_provider')]
    public function test_cast(Type $structure, mixed $value, mixed $expected, ?string $exceptionClass): void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            $structure->cast($value);
        } else {
            static::assertSame($expected, $structure->cast($value));
        }
    }

    public function test_cast_malformed_json_string_payload_chains_json_exception(): void
    {
        try {
            type_structure(['id' => type_integer(), 'name' => type_string()])->cast('{invalid');
            static::fail('Expected CastingException');
        } catch (CastingException $e) {
            static::assertInstanceOf(JsonException::class, $e->getPrevious());
        }
    }

    public function test_cast_missing_required_element_exception_names_the_element(): void
    {
        try {
            type_structure(['id' => type_integer(), 'name' => type_string()])->cast(['id' => 1]);
            static::fail('Expected CastingException');
        } catch (CastingException $e) {
            $previous = $e->getPrevious();
            static::assertInstanceOf(MissingElementCastingException::class, $previous);
            static::assertSame('name', $previous->element);
        }
    }

    public function test_cast_nested_structure_missing_element_chains_through_both_levels(): void
    {
        try {
            type_structure(['address' => type_structure(['zip' => type_string()])])->cast(['address' => []]);
            static::fail('Expected CastingException');
        } catch (CastingException $e) {
            $addressLevel = $e->getPrevious();
            static::assertInstanceOf(CastingException::class, $addressLevel);
            $elementLevel = $addressLevel->getPrevious();
            static::assertInstanceOf(MissingElementCastingException::class, $elementLevel);
            static::assertSame('zip', $elementLevel->element);
        }
    }

    public function test_constructor_accepts_all_optional_elements(): void
    {
        $type = type_structure([
            'id' => structure_element('id', type_integer(), optional: true),
            'name' => structure_element('name', type_string(), optional: true),
        ]);

        static::assertSame('structure{id?: integer, name?: string}', $type->toString());
    }

    public function test_structure_element_values_delegate_to_the_constructor(): void
    {
        static::assertEquals(
            new StructureType([
                new StructureElement('id', type_integer()),
                new StructureElement('nick', type_string(), optional: true),
            ], true),
            type_structure([
                'id' => type_integer(),
                'nick' => structure_element('nick', type_string(), optional: true),
            ], allow_extra: true),
        );
    }

    public function test_structure_element_value_must_match_its_key(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Structure element name "nick" does not match its key "nickname"');

        type_structure(['nickname' => structure_element('nick', type_string(), optional: true)]);
    }

    public function test_structure_element_with_numeric_string_name_matches_its_coerced_key(): void
    {
        // PHP coerces the array literal key '0' to int(0) while the marker keeps the string name.
        $type = type_structure(['0' => structure_element('0', type_integer(), optional: true)]);

        static::assertSame('structure{0?: integer}', $type->toString());
    }

    public function test_constructor_rejects_duplicate_element_names(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Structure element names must be unique: id');

        new StructureType([
            new StructureElement('id', type_integer()),
            new StructureElement('id', type_string()),
        ]);
    }

    public function test_element_lookup_finds_by_name(): void
    {
        $type = new StructureType([
            new StructureElement('id', type_integer()),
            new StructureElement('name', type_string(), optional: true),
        ]);

        $element = $type->element('name');

        static::assertNotNull($element);
        static::assertSame('name', $element->name);
        static::assertTrue($element->optional);
        static::assertEquals(type_string(), $element->type);
    }

    public function test_element_lookup_returns_null_for_unknown_name(): void
    {
        static::assertNull((new StructureType([new StructureElement('id', type_integer())]))->element('name'));
    }

    public function test_constructor_requires_at_least_one_element(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Structure must receive at least one element (required or optional)');

        type_structure([]);
    }

    public function test_elements(): void
    {
        static::assertEquals(
            [structure_element('map', type_map(type_string(), type_float()))],
            type_structure(['map' => type_map(type_string(), type_float())])->elements(),
        );
    }

    #[DataProvider('is_valid_data_provider')]
    public function test_is_valid(Type $structure, mixed $value, bool $expected): void
    {
        static::assertSame($expected, $structure->isValid($value));
    }

    public function test_normalization(): void
    {
        $type = type_structure([
            'string' => type_string(),
            'float' => type_float(),
            'map' => type_map(type_string(), type_list(type_datetime())),
        ]);
        $normalized = $type->normalize();
        $recreated = type_from_array($normalized);

        static::assertEquals($type, $recreated);
    }

    public function test_normalization_with_allow_extra(): void
    {
        $type = type_structure(['string' => type_string(), 'float' => type_float()], true);
        $normalized = $type->normalize();
        $recreated = StructureType::fromArray($normalized);

        static::assertEquals($type, $recreated);
        static::assertTrue($recreated->allowsExtra());
    }

    public function test_normalization_with_optional_elements(): void
    {
        $type = type_structure([
            'id' => type_integer(),
            'name' => structure_element('name', type_string(), optional: true),
            'active' => structure_element('active', type_boolean(), optional: true),
        ]);
        $normalized = $type->normalize();
        $recreated = StructureType::fromArray($normalized);

        static::assertEquals($type, $recreated);
        static::assertTrue($recreated->element('name')?->optional);
        static::assertTrue($recreated->element('active')?->optional);
    }

    public function test_optional_flag_defaults_to_false(): void
    {
        static::assertFalse((new StructureType([new StructureElement('id', type_integer())]))->element(
            'id',
        )?->optional);
    }

    public function test_normalization_round_trip_preserves_interleaved_field_order(): void
    {
        $type = type_structure([
            'z' => type_integer(),
            'a' => structure_element('a', type_integer(), optional: true),
            'b' => type_string(),
        ]);

        $recreated = StructureType::fromArray($type->normalize());

        static::assertEquals($type, $recreated);
        static::assertSame('structure{z: integer, a?: integer, b: string}', $recreated->toString());
    }

    public function test_normalization_round_trip_preserves_numeric_element_name(): void
    {
        $type = type_structure([0 => type_integer(), 'b' => type_string()]);
        $normalized = $type->normalize();

        static::assertSame(
            '{"type":"structure_v2","fields":[{"name":"0","type":{"type":"integer"},"optional":false},{"name":"b","type":{"type":"string"},"optional":false}],"allow_extra":false}',
            json_encode($normalized, JSON_THROW_ON_ERROR),
        );

        $recreated = StructureType::fromArray($normalized);

        static::assertEquals($type, $recreated);
        static::assertSame('structure{0: integer, b: string}', $recreated->toString());
        static::assertSame(0, $recreated->element(0)?->name);
        static::assertNull($recreated->element('0'));
    }

    public function test_from_array_rejects_legacy_two_bucket_shape(): void
    {
        $this->expectException(InvalidTypeException::class);

        StructureType::fromArray([
            'type' => 'structure',
            'elements' => ['id' => ['type' => 'integer']],
            'optional_elements' => [],
            'allow_extra' => false,
        ]);
    }

    public function test_cast_emits_interleaved_declared_order(): void
    {
        $casted = type_structure([
            'z' => type_integer(),
            'a' => structure_element('a', type_integer(), optional: true),
            'b' => type_string(),
        ])->cast(['b' => 'x', 'a' => 1, 'z' => 2]);

        static::assertSame(['z', 'a', 'b'], array_keys($casted));
        static::assertSame(['z' => 2, 'a' => 1, 'b' => 'x'], $casted);
    }

    #[DataProvider('required_and_optional_presence_matrix')]
    public function test_presence_and_null_are_distinct_facts(Type $type, mixed $value, bool $valid): void
    {
        static::assertSame($valid, $type->isValid($value));
    }

    /**
     * The 2x2 matrix per field kind: present/absent x null/non-null. `bool $optional` means the
     * field may be ABSENT; a nullable element type means a PRESENT value may be null. The two are
     * independent facts (VALUE_NULL 0x00 vs VALUE_ABSENT 0x03 in the Floe format).
     */
    public static function required_and_optional_presence_matrix(): Generator
    {
        $required = type_structure(['a' => type_integer()]);

        yield 'required, present, non-null' => [$required, ['a' => 1], true];
        yield 'required, present, null' => [$required, ['a' => null], false];
        yield 'required, absent' => [$required, [], false];

        $requiredNullable = type_structure(['a' => type_optional(type_integer())]);

        yield 'required nullable, present, null' => [$requiredNullable, ['a' => null], true];
        yield 'required nullable, absent' => [$requiredNullable, [], false];

        $optional = type_structure([
            'id' => type_integer(),
            'a' => structure_element('a', type_integer(), optional: true),
        ]);

        yield 'optional, present, non-null' => [$optional, ['id' => 1, 'a' => 1], true];
        yield 'optional, present, null' => [$optional, ['id' => 1, 'a' => null], false];
        yield 'optional, absent' => [$optional, ['id' => 1], true];

        $optionalNullable = type_structure([
            'id' => type_integer(),
            'a' => structure_element('a', type_optional(type_integer()), optional: true),
        ]);

        yield 'optional nullable, present, null' => [$optionalNullable, ['id' => 1, 'a' => null], true];
        yield 'optional nullable, absent' => [$optionalNullable, ['id' => 1], true];
    }

    public function test_to_string(): void
    {
        $struct = type_structure([
            'string' => type_string(),
            'float' => type_float(),
            'map' => type_map(type_string(), type_list(type_datetime())),
        ]);

        static::assertSame(
            'structure{string: string, float: float, map: map<string, list<datetime>>}',
            $struct->toString(),
        );
    }

    public function test_to_string_with_optional_elements(): void
    {
        $struct = type_structure([
            'id' => type_integer(),
            'name' => type_string(),
            'active' => structure_element('active', type_boolean(), optional: true),
            'score' => structure_element('score', type_float(), optional: true),
        ]);

        static::assertSame(
            'structure{id: integer, name: string, active?: boolean, score?: float}',
            $struct->toString(),
        );
    }
}
