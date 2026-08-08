<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row;

use ArrayIterator;
use DateInterval;
use DateTimeImmutable;
use DateTimeZone;
use Dom\HTMLDocument;
use DOMDocument;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Entry\TimeEntry;
use Flow\ETL\Row\EntryFactory;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Tests\Fixtures\Enum\BackedIntEnum;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Type\Native\UnionType;
use Flow\Types\Value\Json;
use Flow\Types\Value\Uuid as FlowUuid;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\RequiresPhp;
use Ramsey\Uuid\Uuid;

use function class_exists;
use function Flow\ETL\DSL\bool_entry;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\date_entry;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\datetime_entry;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\enum_entry;
use function Flow\ETL\DSL\enum_schema;
use function Flow\ETL\DSL\float_entry;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\html_element_entry;
use function Flow\ETL\DSL\html_element_schema;
use function Flow\ETL\DSL\html_entry;
use function Flow\ETL\DSL\html_schema;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\integer_schema;
use function Flow\ETL\DSL\json_entry;
use function Flow\ETL\DSL\json_object_entry;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\list_entry;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\map_entry;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\null_entry;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\string_entry;
use function Flow\ETL\DSL\string_schema;
use function Flow\ETL\DSL\structure_entry;
use function Flow\ETL\DSL\structure_schema;
use function Flow\ETL\DSL\time_entry;
use function Flow\ETL\DSL\time_schema;
use function Flow\ETL\DSL\union_schema;
use function Flow\ETL\DSL\uuid_entry;
use function Flow\ETL\DSL\uuid_schema;
use function Flow\ETL\DSL\xml_entry;
use function Flow\ETL\DSL\xml_schema;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_time_zone;
use function Flow\Types\DSL\type_union;
use function Flow\Types\DSL\type_uuid;

final class EntryFactoryTest extends FlowTestCase
{
    public static function provide_definition_cases(): Generator
    {
        yield 'boolean' => [bool_schema('e'), true, bool_entry('e', true)];
        yield 'date' => [date_schema('e'), $date = new DateTimeImmutable('2024-04-01'), date_entry('e', $date)];
        yield 'datetime' => [
            datetime_schema('e'),
            $datetime = new DateTimeImmutable('2024-04-01 10:00:00 UTC'),
            datetime_entry('e', $datetime),
        ];
        yield 'enum' => [
            enum_schema('e', BackedIntEnum::class),
            BackedIntEnum::one,
            enum_entry('e', BackedIntEnum::one),
        ];
        yield 'float' => [float_schema('e'), 1.5, float_entry('e', 1.5)];
        yield 'integer' => [integer_schema('e'), 1, int_entry('e', 1)];
        yield 'json' => [json_schema('e'), $json = new Json('{"id":1}'), json_entry('e', $json)];
        yield 'list' => [
            list_schema('e', type_list(type_integer())),
            [1, 2, 3],
            list_entry('e', [1, 2, 3], type_list(type_integer())),
        ];
        yield 'map' => [
            map_schema('e', type_map(type_string(), type_integer())),
            ['a' => 1],
            map_entry('e', ['a' => 1], type_map(type_string(), type_integer())),
        ];
        yield 'string' => [string_schema('e'), 'flow', string_entry('e', 'flow')];
        yield 'structure' => [
            structure_schema('e', type_structure(['a' => type_integer()])),
            ['a' => 1],
            structure_entry('e', ['a' => 1], type_structure(['a' => type_integer()])),
        ];
        yield 'time' => [time_schema('e'), $time = new DateInterval('PT1H'), time_entry('e', $time)];
        yield 'uuid' => [
            uuid_schema('e'),
            $uuid = new FlowUuid('f47ac10b-58cc-4372-a567-0e02b2c3d479'),
            uuid_entry('e', $uuid),
        ];

        $xmlDocument = new DOMDocument();
        $xmlDocument->loadXML('<root><foo>1</foo></root>');

        yield 'xml' => [xml_schema('e'), $xmlDocument, xml_entry('e', $xmlDocument)];
    }

    public static function provide_recognized_data(): Generator
    {
        yield 'json' => ['{"id":1}', string_entry('e', '{"id":1}')];
        yield 'xml' => [
            $xml = '<root><foo>1</foo><bar>2</bar><baz>3</baz></root>',
            string_entry('e', $xml),
        ];
        yield 'html' => [
            $html = '<!DOCTYPE html><html lang="en"><head></head><body><div id="id">2</div><p>3</p></body></html>',
            string_entry('e', $html),
        ];
        yield 'uuid' => [
            $uuid = '00000000-0000-0000-0000-000000000000',
            string_entry('e', $uuid),
        ];
    }

    public static function provide_unrecognized_data(): Generator
    {
        yield 'json alike' => ['{"id":1'];
        yield 'uuid alike' => ['00000000-0000-0000-0000-00000'];
        yield 'xml alike' => ['<root'];
        yield 'html alike' => ['<html'];
        yield 'space' => [' '];
        yield 'new line' => ["\n"];
        yield 'invisible' => ['‎ '];
    }

    public function test_array_structure(): void
    {
        static::assertEquals(structure_entry('e', ['a' => 1, 'b' => '2'], type_structure([
            'a' => type_integer(),
            'b' => type_string(),
        ])), (new EntryFactory())->create('e', ['a' => 1, 'b' => '2']));
    }

    public function test_bool(): void
    {
        static::assertEquals(bool_entry('e', false), (new EntryFactory())->create('e', false));
    }

    public function test_boolean_with_definition(): void
    {
        $definition = bool_schema('e');
        static::assertEquals(
            bool_entry('e', false),
            (new EntryFactory())->cast('e', false, $definition->type(), $definition->metadata()),
        );
    }

    public function test_coerce_null_with_non_optional_type_keeps_silent_null(): void
    {
        static::assertEquals(int_entry('e', null), (new EntryFactory())->cast('e', null, type_integer()));
    }

    public function test_create_null_with_type_creates_typed_null(): void
    {
        static::assertEquals(int_entry('e', null), (new EntryFactory())->create('e', null, type_integer()));
    }

    public function test_create_trusted_native_value_with_type(): void
    {
        $datetime = new DateTimeImmutable('2024-04-01 10:00:00 UTC');
        static::assertEquals(
            datetime_entry('e', $datetime),
            (new EntryFactory())->create('e', $datetime, type_datetime()),
        );
    }

    public function test_create_trusted_scalar_with_type(): void
    {
        static::assertEquals(int_entry('e', 5), (new EntryFactory())->create('e', 5, type_integer()));
    }

    public function test_create_with_array_type_and_null_value(): void
    {
        static::assertEquals(json_entry('e', null), (new EntryFactory())->create('e', null, type_array()));
    }

    public function test_create_with_array_type_normalizes_object_shaped_value_to_json_object(): void
    {
        static::assertEquals(
            json_object_entry('e', ['a' => 1]),
            (new EntryFactory())->create('e', ['a' => 1], type_array()),
        );
    }

    public function test_create_with_array_type_normalizes_value_to_json(): void
    {
        static::assertEquals(json_entry('e', [1, 2]), (new EntryFactory())->create('e', [1, 2], type_array()));
    }

    public function test_create_with_time_zone_type_normalizes_value_to_string(): void
    {
        static::assertEquals(
            str_entry('e', 'UTC'),
            (new EntryFactory())->create('e', new DateTimeZone('UTC'), type_time_zone()),
        );
    }

    public function test_from_definition_instantiates_a_native_value(): void
    {
        static::assertEquals(int_entry('e', 5), (new EntryFactory())->fromDefinition(integer_schema('e'), 5));
    }

    public function test_from_definition_null_uses_the_nullable_variant(): void
    {
        $entry = (new EntryFactory())->fromDefinition(integer_schema('e'), null);

        static::assertNull($entry->value());
        static::assertTrue($entry->definition()->isNullable());
    }

    public function test_from_definition_preserves_definition_metadata(): void
    {
        static::assertEquals(
            int_entry('e', null, metadata: Metadata::with('k', 1)),
            (new EntryFactory())->fromDefinition(integer_schema('e', true, Metadata::with('k', 1)), null),
        );
    }

    public function test_from_definition_with_union_resolves_the_member_matching_the_value(): void
    {
        /** @var UnionType<mixed, mixed> $type */
        $type = type_union(type_string(), type_integer());

        static::assertEquals(int_entry('e', 42), (new EntryFactory())->fromDefinition(union_schema('e', $type), 42));
    }

    public function test_from_definition_with_union_resolves_the_first_member_for_a_matching_value(): void
    {
        /** @var UnionType<mixed, mixed> $type */
        $type = type_union(type_string(), type_integer());

        static::assertEquals(str_entry('e', 'x'), (new EntryFactory())->fromDefinition(union_schema('e', $type), 'x'));
    }

    public function test_from_definition_with_union_carries_metadata_onto_the_resolved_entry(): void
    {
        /** @var UnionType<mixed, mixed> $type */
        $type = type_union(type_string(), type_integer());

        static::assertEquals(
            int_entry('e', 42, metadata: Metadata::with('k', 1)),
            (new EntryFactory())->fromDefinition(union_schema('e', $type, metadata: Metadata::with('k', 1)), 42),
        );
    }

    public function test_from_definition_with_nullable_union_and_null_value(): void
    {
        /** @var UnionType<mixed, mixed> $type */
        $type = type_union(type_string(), type_integer());

        $entry = (new EntryFactory())->fromDefinition(union_schema('e', $type, true), null);

        static::assertEquals(str_entry('e', null), $entry);
        static::assertTrue($entry->definition()->isNullable());
    }

    public function test_from_definition_with_non_nullable_union_and_null_value(): void
    {
        /** @var UnionType<mixed, mixed> $type */
        $type = type_union(type_string(), type_integer());

        $entry = (new EntryFactory())->fromDefinition(union_schema('e', $type), null);

        static::assertNull($entry->value());
        static::assertTrue($entry->definition()->isNullable());
    }

    public function test_from_definition_with_union_throws_for_a_value_outside_every_member(): void
    {
        /** @var UnionType<mixed, mixed> $type */
        $type = type_union(type_uuid(), type_datetime());

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Entry "e": array value does not match any member of union type');

        (new EntryFactory())->fromDefinition(union_schema('e', $type), [1, 2]);
    }

    public function test_date(): void
    {
        static::assertEquals(
            date_entry('e', '2022-01-01'),
            (new EntryFactory())->create('e', new DateTimeImmutable('2022-01-01')),
        );
    }

    public function test_date_from_int_with_definition(): void
    {
        $definition = date_schema('e');
        static::assertEquals(
            date_entry('e', '1970-01-01'),
            (new EntryFactory())->cast('e', 1, $definition->type(), $definition->metadata()),
        );
    }

    public function test_date_from_null_with_definition(): void
    {
        $definition = date_schema('e', true);
        static::assertEquals(
            date_entry('e', null),
            (new EntryFactory())->cast('e', null, $definition->type(), $definition->metadata()),
        );
    }

    public function test_date_from_string_with_definition(): void
    {
        $definition = date_schema('e');
        static::assertEquals(
            date_entry('e', '2022-01-01'),
            (new EntryFactory())->cast('e', '2022-01-01', $definition->type(), $definition->metadata()),
        );
    }

    public function test_datetime(): void
    {
        static::assertEquals(
            datetime_entry('e', $now = new DateTimeImmutable()),
            (new EntryFactory())->create('e', $now),
        );
    }

    public function test_datetime_string_with_definition(): void
    {
        $definition = datetime_schema('e');
        static::assertEquals(
            datetime_entry('e', '2022-01-01 00:00:00 UTC'),
            (new EntryFactory())->cast('e', '2022-01-01 00:00:00 UTC', $definition->type(), $definition->metadata()),
        );
    }

    public function test_datetime_with_definition(): void
    {
        $definition = datetime_schema('e');
        static::assertEquals(
            datetime_entry('e', $datetime = new DateTimeImmutable('now')),
            (new EntryFactory())->cast('e', $datetime, $definition->type(), $definition->metadata()),
        );
    }

    public function test_enum(): void
    {
        static::assertEquals(enum_entry('e', $enum = BackedIntEnum::one), (new EntryFactory())->create('e', $enum));
    }

    public function test_enum_from_string_with_definition(): void
    {
        $definition = enum_schema('e', BackedIntEnum::class);
        static::assertEquals(
            enum_entry('e', BackedIntEnum::one),
            (new EntryFactory())->cast('e', 1, $definition->type(), $definition->metadata()),
        );
    }

    public function test_enum_invalid_value_with_definition(): void
    {
        $definition = enum_schema('e', BackedIntEnum::class);
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Entry "e" conversion exception. Can\'t cast "string" into "enum<Flow\ETL\Tests\Fixtures\Enum\BackedIntEnum>" type',
        );
        (new EntryFactory())->cast('e', 'invalid', $definition->type(), $definition->metadata());
    }

    public function test_float(): void
    {
        static::assertEquals(float_entry('e', 1.1), (new EntryFactory())->create('e', 1.1));
    }

    public function test_float_with_definition(): void
    {
        $definition = float_schema('e');
        static::assertEquals(
            float_entry('e', 1.1),
            (new EntryFactory())->cast('e', 1.1, $definition->type(), $definition->metadata()),
        );
    }

    public function test_float_with_definition_and_metadata(): void
    {
        $definition = float_schema('e', metadata: Metadata::with('test', 1));
        static::assertEquals(
            float_entry('e', 1.1, metadata: Metadata::with('test', 1)),
            (new EntryFactory())->cast('e', 1.1, $definition->type(), $definition->metadata()),
        );
    }

    public function test_from_empty_string(): void
    {
        static::assertEquals(str_entry('e', ''), (new EntryFactory())->create('e', ''));
    }

    public function test_html_element_from_string(): void
    {
        static::assertEquals(
            string_entry('e', $html = '<div>2</div><p>bar</p>'),
            (new EntryFactory())->create('e', $html),
        );
    }

    #[RequiresPhp('>= 8.4.0')]
    public function test_html_element_string_with_definition(): void
    {
        $definition = html_element_schema('e');
        static::assertEquals(
            html_element_entry('e', $html = '<div>2</div><p>bar</p>'),
            (new EntryFactory())->cast('e', $html, $definition->type(), $definition->metadata()),
        );
    }

    #[RequiresPhp('>= 8.4.0')]
    public function test_html_from_dom_html_document(): void
    {
        // @mago-ignore analysis:unavailable-method
        $doc = HTMLDocument::createFromString(
            $html = '<!DOCTYPE html><html lang="en"><head></head><body><div>2</div><p>3</p></body></html>',
        );
        static::assertEquals(html_entry('e', $html), (new EntryFactory())->create('e', $doc));
    }

    public function test_html_from_string(): void
    {
        static::assertEquals(
            string_entry(
                'e',
                $html = '<!DOCTYPE html><html lang="en"><head></head><body><div>foo</div><p>3</p></body></html>',
            ),
            (new EntryFactory())->create('e', $html),
        );
    }

    #[RequiresPhp('>= 8.4.0')]
    public function test_html_string_with_definition(): void
    {
        // @mago-ignore analysis:unavailable-method
        $document = HTMLDocument::createFromString(
            $html = '<!DOCTYPE html><html lang="en"><head></head><body><div>2</div><p>bar</p></body></html>',
        );
        $definition = html_schema('e');
        static::assertEquals(
            html_entry('e', $html),
            (new EntryFactory())->cast('e', $document, $definition->type(), $definition->metadata()),
        );
    }

    /**
     * @param Definition<mixed> $definition
     * @param Entry<mixed> $expected
     */
    #[DataProvider('provide_definition_cases')]
    public function test_coerce_with_definition(Definition $definition, mixed $value, Entry $expected): void
    {
        static::assertEquals($expected, (new EntryFactory())->cast(
            'e',
            $value,
            $definition->type(),
            $definition->metadata(),
        ));
    }

    public function test_int(): void
    {
        static::assertEquals(int_entry('e', 1), (new EntryFactory())->create('e', 1));
    }

    public function test_integer_with_definition(): void
    {
        $definition = integer_schema('e');
        static::assertEquals(
            int_entry('e', 1),
            (new EntryFactory())->cast('e', 1, $definition->type(), $definition->metadata()),
        );
    }

    public function test_integer_with_definition_and_metadata(): void
    {
        $definition = integer_schema('e', metadata: Metadata::with('test', 1));
        static::assertEquals(
            int_entry('e', 1, metadata: Metadata::with('test', 1)),
            (new EntryFactory())->cast('e', 1, $definition->type(), $definition->metadata()),
        );
    }

    public function test_json(): void
    {
        static::assertEquals(str_entry('e', '{}'), (new EntryFactory())->create('e', '{}'));
    }

    public function test_json_object(): void
    {
        static::assertEquals(str_entry('e', '{"id":1}'), (new EntryFactory())->create('e', '{"id":1}'));
    }

    public function test_json_object_array_with_definition(): void
    {
        $definition = json_schema('e');
        static::assertEquals(
            json_object_entry('e', ['id' => 1]),
            (new EntryFactory())->cast('e', ['id' => 1], $definition->type(), $definition->metadata()),
        );
    }

    public function test_json_string(): void
    {
        static::assertEquals(str_entry('e', '{"id": 1}'), (new EntryFactory())->create('e', '{"id": 1}'));
    }

    public function test_json_string_with_definition(): void
    {
        $definition = json_schema('e');
        static::assertEquals(
            json_entry('e', '{"id": 1}'),
            (new EntryFactory())->cast('e', '{"id": 1}', $definition->type(), $definition->metadata()),
        );
    }

    public function test_json_with_definition(): void
    {
        $definition = json_schema('e');
        static::assertEquals(
            json_entry('e', [['id' => 1]]),
            (new EntryFactory())->cast('e', [['id' => 1]], $definition->type(), $definition->metadata()),
        );
    }

    public function test_list_int_with_definition(): void
    {
        $definition = list_schema('e', type_list(type_integer()));
        static::assertEquals(
            list_entry('e', [1, 2, 3], type_list(type_integer())),
            (new EntryFactory())->cast('e', [1, 2, 3], $definition->type(), $definition->metadata()),
        );
    }

    public function test_list_int_with_definition_but_string_list(): void
    {
        $definition = list_schema('e', type_list(type_string()));
        static::assertEquals(
            list_entry('e', ['false', 'true', 'true'], type_list(type_string())),
            (new EntryFactory())->cast('e', [false, true, true], $definition->type(), $definition->metadata()),
        );
    }

    public function test_list_of_datetime_with_definition(): void
    {
        $definition = list_schema('e', type_list(type_datetime()));
        static::assertEquals(
            list_entry(
                'e',
                $list = [new DateTimeImmutable('now'), new DateTimeImmutable('tomorrow')],
                type_list(type_datetime()),
            ),
            (new EntryFactory())->cast('e', $list, $definition->type(), $definition->metadata()),
        );
    }

    public function test_list_of_datetimes(): void
    {
        static::assertEquals(
            list_entry('e', $list = [new DateTimeImmutable(), new DateTimeImmutable()], type_list(type_datetime())),
            (new EntryFactory())->create('e', $list),
        );
    }

    public function test_list_of_scalars(): void
    {
        static::assertEquals(
            list_entry('e', [1, 2], type_list(type_integer())),
            (new EntryFactory())->create('e', [1, 2]),
        );
    }

    public function test_nested_structure(): void
    {
        static::assertEquals(
            structure_entry(
                'address',
                [
                    'city' => 'Krakow',
                    'geo' => [
                        'lat' => 50.06143,
                        'lon' => 19.93658,
                    ],
                    'street' => 'Floriańska',
                    'zip' => '31-021',
                ],
                type_structure([
                    'city' => type_string(),
                    'geo' => type_structure([
                        'lat' => type_float(),
                        'lon' => type_float(),
                    ]),
                    'street' => type_string(),
                    'zip' => type_string(),
                ]),
            ),
            (new EntryFactory())->create('address', [
                'city' => 'Krakow',
                'geo' => [
                    'lat' => 50.06143,
                    'lon' => 19.93658,
                ],
                'street' => 'Floriańska',
                'zip' => '31-021',
            ]),
        );
    }

    public function test_null_without_definition_creates_null_entry(): void
    {
        static::assertEquals(null_entry('e'), (new EntryFactory())->create('e', null));
    }

    public function test_object(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            "e: object<ArrayIterator> can't be converted to any known Entry, please normalize that object first",
        );
        (new EntryFactory())->create('e', new ArrayIterator([1, 2]));
    }

    /**
     * @param Entry<mixed> $entry
     */
    #[DataProvider('provide_recognized_data')]
    public function test_recognized_data_set_same_as_provided(string $input, Entry $entry): void
    {
        static::assertEquals($entry, (new EntryFactory())->create('e', $input));
    }

    public function test_string(): void
    {
        static::assertEquals(str_entry('e', 'test'), (new EntryFactory())->create('e', 'test'));
    }

    public function test_string_with_definition(): void
    {
        $definition = string_schema('e');
        static::assertEquals(
            str_entry('e', 'string'),
            (new EntryFactory())->cast('e', 'string', $definition->type(), $definition->metadata()),
        );
    }

    public function test_structure(): void
    {
        static::assertEquals(
            structure_entry(
                'address',
                ['id' => 1, 'city' => 'Krakow', 'street' => 'Floriańska', 'zip' => '31-021'],
                type_structure([
                    'id' => type_integer(),
                    'city' => type_string(),
                    'street' => type_string(),
                    'zip' => type_string(),
                ]),
            ),
            (new EntryFactory())->create('address', [
                'id' => 1,
                'city' => 'Krakow',
                'street' => 'Floriańska',
                'zip' => '31-021',
            ]),
        );
    }

    public function test_time(): void
    {
        static::assertEquals(TimeEntry::fromDays('e', 1), (new EntryFactory())->create('e', new DateInterval('P1D')));
    }

    public function test_time_from_null_with_definition(): void
    {
        $definition = time_schema('e', true);
        static::assertEquals(
            time_entry('e', null),
            (new EntryFactory())->cast('e', null, $definition->type(), $definition->metadata()),
        );
    }

    public function test_time_from_string_with_definition(): void
    {
        $definition = time_schema('e');
        static::assertEquals(
            time_entry('e', new DateInterval('P10D')),
            (new EntryFactory())->cast('e', 'P10D', $definition->type(), $definition->metadata()),
        );
    }

    public function test_timezone_creates_string_entry(): void
    {
        static::assertEquals(str_entry('e', 'UTC'), (new EntryFactory())->create('e', new DateTimeZone('UTC')));
    }

    public function test_union_with_definition_falls_back_to_first_castable_member(): void
    {
        $definition = union_schema('e', type_union(type_integer(), type_string()));
        static::assertEquals(
            int_entry('e', 1),
            (new EntryFactory())->cast('e', true, $definition->type(), $definition->metadata()),
        );
    }

    public function test_union_with_definition_keeps_numeric_string_as_string(): void
    {
        $definition = union_schema('e', type_union(type_integer(), type_string()));
        static::assertEquals(
            str_entry('e', '123'),
            (new EntryFactory())->cast('e', '123', $definition->type(), $definition->metadata()),
        );
    }

    public function test_union_with_definition_resolves_to_complex_member(): void
    {
        $definition = union_schema('e', type_union(type_list(type_integer()), type_string()));
        static::assertEquals(
            list_entry('e', [1, 2, 3], type_list(type_integer())),
            (new EntryFactory())->cast('e', [1, 2, 3], $definition->type(), $definition->metadata()),
        );
    }

    public function test_union_with_definition_resolves_to_integer(): void
    {
        $definition = union_schema('e', type_union(type_integer(), type_string()));
        static::assertEquals(
            int_entry('e', 1),
            (new EntryFactory())->cast('e', 1, $definition->type(), $definition->metadata()),
        );
    }

    public function test_union_with_definition_resolves_to_string(): void
    {
        $definition = union_schema('e', type_union(type_integer(), type_string()));
        static::assertEquals(
            str_entry('e', 'flow'),
            (new EntryFactory())->cast('e', 'flow', $definition->type(), $definition->metadata()),
        );
    }

    public function test_union_with_definition_with_null_value_creates_first_member_entry(): void
    {
        $definition = union_schema('e', type_union(type_integer(), type_string()), true);
        static::assertEquals(
            int_entry('e', null),
            (new EntryFactory())->cast('e', null, $definition->type(), $definition->metadata()),
        );
    }

    #[DataProvider('provide_unrecognized_data')]
    public function test_unrecognized_data_set_same_as_provided(string $input): void
    {
        static::assertEquals(str_entry('e', $input), (new EntryFactory())->create('e', $input));
    }

    public function test_uuid_from_ramsey_uuid_library(): void
    {
        if (!class_exists(Uuid::class)) {
            static::markTestSkipped("Package 'ramsey/uuid' is required for this test.");
        }
        $uuidObject = Uuid::uuid4();
        static::assertEquals(uuid_entry('e', $uuidObject->toString()), (new EntryFactory())->create('e', $uuidObject));
    }

    public function test_uuid_from_string(): void
    {
        static::assertEquals(
            string_entry('e', $uuid = '00000000-0000-0000-0000-000000000000'),
            (new EntryFactory())->create('e', $uuid),
        );
    }

    public function test_uuid_string_with_definition(): void
    {
        $definition = uuid_schema('e');
        static::assertEquals(
            uuid_entry('e', $uuid = '00000000-0000-0000-0000-000000000000'),
            (new EntryFactory())->cast('e', $uuid, $definition->type(), $definition->metadata()),
        );
    }

    public function test_uuid_type(): void
    {
        static::assertEquals(
            string_entry('e', '00000000-0000-0000-0000-000000000000'),
            (new EntryFactory())->create('e', '00000000-0000-0000-0000-000000000000'),
        );
    }

    public function test_xml_from_dom_document(): void
    {
        $doc = new DOMDocument();
        $doc->loadXML($xml = '<root><foo>1</foo><bar>2</bar><baz>3</baz></root>');
        static::assertEquals(xml_entry('e', $xml), (new EntryFactory())->create('e', $doc));
    }

    public function test_xml_from_string(): void
    {
        static::assertEquals(
            string_entry('e', $xml = '<root><foo>1</foo><bar>2</bar><baz>3</baz></root>'),
            (new EntryFactory())->create('e', $xml),
        );
    }

    public function test_xml_string_with_definition(): void
    {
        $definition = xml_schema('e');
        static::assertEquals(
            xml_entry('e', $xml = '<root><foo>1</foo><bar>2</bar><baz>3</baz></root>'),
            (new EntryFactory())->cast('e', $xml, $definition->type(), $definition->metadata()),
        );
    }
}
