<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row;

use function Flow\ETL\DSL\{bool_entry,
    date_entry,
    datetime_entry,
    enum_entry,
    float_entry,
    int_entry,
    json_entry,
    json_object_entry,
    list_entry,
    str_entry,
    time_entry,
    uuid_entry,
    xml_entry};
use function Flow\ETL\DSL\{bool_schema,
    date_schema,
    datetime_schema,
    empty_schema,
    enum_schema,
    float_schema,
    integer_schema,
    json_schema,
    list_schema,
    schema,
    string_schema,
    structure_entry,
    time_schema,
    uuid_schema,
    xml_schema};
use function Flow\Types\DSL\{type_datetime, type_float, type_integer, type_list, type_map, type_string, type_structure};
use Flow\ETL\Exception\{InvalidArgumentException, SchemaDefinitionNotFoundException};
use Flow\ETL\Row\Entry\{TimeEntry};
use Flow\ETL\Row\{EntryFactory};
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Tests\Fixtures\Enum\BackedIntEnum;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\DataProvider;
use Ramsey\Uuid\Uuid;

final class EntryFactoryTest extends FlowTestCase
{
    public static function provide_unrecognized_data() : \Generator
    {
        yield 'json alike' => [
            '{"id":1',
        ];

        yield 'uuid alike' => [
            '00000000-0000-0000-0000-00000',
        ];

        yield 'xml alike' => [
            '<root',
        ];

        yield 'space' => [
            ' ',
        ];

        yield 'new line' => [
            "\n",
        ];

        yield 'invisible' => [
            '‎ ',
        ];
    }

    public function test_array_structure() : void
    {
        self::assertEquals(
            structure_entry('e', ['a' => 1, 'b' => '2'], type_structure(['a' => type_integer(), 'b' => type_string()])),
            (new EntryFactory())->create('e', ['a' => 1, 'b' => '2'])
        );
    }

    public function test_bool() : void
    {
        self::assertEquals(
            bool_entry('e', false),
            (new EntryFactory())->create('e', false)
        );
    }

    public function test_boolean_with_schema() : void
    {
        self::assertEquals(
            bool_entry('e', false),
            (new EntryFactory())->create('e', false, schema(bool_schema('e')))
        );
    }

    public function test_date() : void
    {
        self::assertEquals(
            date_entry('e', '2022-01-01'),
            (new EntryFactory())->create('e', new \DateTimeImmutable('2022-01-01'))
        );
    }

    public function test_date_from_int_with_definition() : void
    {
        self::assertEquals(
            date_entry('e', '1970-01-01'),
            (new EntryFactory())->create('e', 1, schema(date_schema('e')))
        );
    }

    public function test_date_from_null_with_definition() : void
    {
        self::assertEquals(
            date_entry('e', null),
            (new EntryFactory())->create('e', null, schema(date_schema('e', true)))
        );
    }

    public function test_date_from_string_with_definition() : void
    {
        self::assertEquals(
            date_entry('e', '2022-01-01'),
            (new EntryFactory())->create('e', '2022-01-01', schema(date_schema('e')))
        );
    }

    public function test_datetime() : void
    {
        self::assertEquals(
            datetime_entry('e', $now = new \DateTimeImmutable()),
            (new EntryFactory())->create('e', $now)
        );
    }

    public function test_datetime_string_with_schema() : void
    {
        self::assertEquals(
            datetime_entry('e', '2022-01-01 00:00:00 UTC'),
            (new EntryFactory())
                ->create('e', '2022-01-01 00:00:00 UTC', schema(datetime_schema('e')))
        );
    }

    public function test_datetime_with_schema() : void
    {
        self::assertEquals(
            datetime_entry('e', $datetime = new \DateTimeImmutable('now')),
            (new EntryFactory())
                ->create('e', $datetime, schema(datetime_schema('e')))
        );
    }

    public function test_enum() : void
    {
        self::assertEquals(
            enum_entry('e', $enum = BackedIntEnum::one),
            (new EntryFactory())
                ->create('e', $enum)
        );
    }

    public function test_enum_from_string_with_schema() : void
    {
        self::assertEquals(
            enum_entry('e', BackedIntEnum::one),
            (new EntryFactory())
                ->create('e', 1, schema(enum_schema('e', BackedIntEnum::class)))
        );
    }

    public function test_enum_invalid_value_with_schema() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Entry "e" conversion exception. Can\'t cast "string" into "enum<Flow\ETL\Tests\Fixtures\Enum\BackedIntEnum>" type');

        (new EntryFactory())
            ->create('e', 'invalid', schema(enum_schema('e', BackedIntEnum::class)));
    }

    public function test_float() : void
    {
        self::assertEquals(
            float_entry('e', 1.1),
            (new EntryFactory())->create('e', 1.1)
        );
    }

    public function test_float_with_schema() : void
    {
        self::assertEquals(
            float_entry('e', 1.1),
            (new EntryFactory())->create('e', 1.1, schema(float_schema('e')))
        );
    }

    public function test_float_with_schema_and_metadata() : void
    {
        self::assertEquals(
            float_entry('e', 1.1, metadata: Metadata::with('test', 1)),
            (new EntryFactory())->create('e', 1.1, schema(float_schema('e', metadata: Metadata::with('test', 1))))
        );
    }

    public function test_from_empty_string() : void
    {
        self::assertEquals(
            str_entry('e', ''),
            (new EntryFactory())->create('e', '')
        );
    }

    public function test_int() : void
    {
        self::assertEquals(
            int_entry('e', 1),
            (new EntryFactory())->create('e', 1)
        );
    }

    public function test_integer_with_schema() : void
    {
        self::assertEquals(
            int_entry('e', 1),
            (new EntryFactory())->create('e', 1, schema(integer_schema('e')))
        );
    }

    public function test_integer_with_schema_and_metadata() : void
    {
        self::assertEquals(
            int_entry('e', 1, metadata: Metadata::with('test', 1)),
            (new EntryFactory())->create('e', 1, schema(integer_schema('e', metadata: Metadata::with('test', 1))))
        );
    }

    public function test_json() : void
    {
        self::assertEquals(
            json_entry('e', '{}'),
            (new EntryFactory())->create('e', '{}')
        );
    }

    public function test_json_object() : void
    {
        self::assertEquals(
            json_object_entry('e', ['id' => 1]),
            (new EntryFactory())->create('e', '{"id":1}')
        );
    }

    public function test_json_object_array_with_schema() : void
    {
        self::assertEquals(
            json_object_entry('e', ['id' => 1]),
            (new EntryFactory())->create('e', ['id' => 1], schema(json_schema('e')))
        );
    }

    public function test_json_string() : void
    {
        self::assertEquals(
            json_entry('e', '{"id": 1}'),
            (new EntryFactory())->create('e', '{"id": 1}')
        );
    }

    public function test_json_string_with_schema() : void
    {
        self::assertEquals(
            json_entry('e', '{"id": 1}'),
            (new EntryFactory())->create('e', '{"id": 1}', schema(json_schema('e')))
        );
    }

    public function test_json_with_schema() : void
    {
        self::assertEquals(
            json_entry('e', [['id' => 1]]),
            (new EntryFactory())->create('e', [['id' => 1]], schema(json_schema('e')))
        );
    }

    public function test_list_int_with_schema() : void
    {
        self::assertEquals(
            list_entry('e', [1, 2, 3], type_list(type_integer())),
            (new EntryFactory())->create('e', [1, 2, 3], schema(list_schema('e', type_list(type_integer()))))
        );
    }

    public function test_list_int_with_schema_but_string_list() : void
    {
        self::assertEquals(
            list_entry('e', ['false', 'true', 'true'], type_list(type_string())),
            (new EntryFactory())->create('e', [false, true, true], schema(list_schema('e', type_list(type_string()))))
        );
    }

    public function test_list_of_datetime_with_schema() : void
    {
        self::assertEquals(
            list_entry('e', $list = [new \DateTimeImmutable('now'), new \DateTimeImmutable('tomorrow')], type_list(type_datetime())),
            (new EntryFactory())
                ->create('e', $list, schema(list_schema('e', type_list(type_datetime()))))
        );
    }

    public function test_list_of_datetimes() : void
    {
        self::assertEquals(
            list_entry('e', $list = [new \DateTimeImmutable(), new \DateTimeImmutable()], type_list(type_datetime())),
            (new EntryFactory())->create('e', $list)
        );
    }

    public function test_list_of_scalars() : void
    {
        self::assertEquals(
            list_entry('e', [1, 2], type_list(type_integer())),
            (new EntryFactory())->create('e', [1, 2])
        );
    }

    public function test_nested_structure() : void
    {
        self::assertEquals(
            structure_entry('address', [
                'city' => 'Krakow',
                'geo' => [
                    'lat' => 50.06143,
                    'lon' => 19.93658,
                ],
                'street' => 'Floriańska',
                'zip' => '31-021',
            ], type_structure([
                'city' => type_string(),
                'geo' => type_map(type_string(), type_float()),
                'street' => type_string(),
                'zip' => type_string(),
            ])),
            (new EntryFactory())->create('address', [
                'city' => 'Krakow',
                'geo' => [
                    'lat' => 50.06143,
                    'lon' => 19.93658,
                ],
                'street' => 'Floriańska',
                'zip' => '31-021',
            ])
        );
    }

    public function test_object() : void
    {
        $this->expectExceptionMessage("e: object<ArrayIterator> can't be converted to any known Entry, please normalize that object first");

        (new EntryFactory())->create('e', new \ArrayIterator([1, 2]));
    }

    public function test_string() : void
    {
        self::assertEquals(
            str_entry('e', 'test'),
            (new EntryFactory())->create('e', 'test')
        );
    }

    public function test_string_with_schema() : void
    {
        self::assertEquals(
            str_entry('e', 'string'),
            (new EntryFactory())->create('e', 'string', schema(string_schema('e')))
        );
    }

    public function test_structure() : void
    {
        self::assertEquals(
            structure_entry('address', ['id' => 1, 'city' => 'Krakow', 'street' => 'Floriańska', 'zip' => '31-021'], type_structure([
                'id' => type_integer(),
                'city' => type_string(),
                'street' => type_string(),
                'zip' => type_string(),
            ])),
            (new EntryFactory())->create('address', ['id' => 1, 'city' => 'Krakow', 'street' => 'Floriańska', 'zip' => '31-021'])
        );
    }

    public function test_time() : void
    {
        self::assertEquals(
            TimeEntry::fromDays('e', 1),
            (new EntryFactory())->create('e', new \DateInterval('P1D'))
        );
    }

    public function test_time_from_null_with_definition() : void
    {
        self::assertEquals(
            time_entry('e', null),
            (new EntryFactory())->create('e', null, schema(time_schema('e', true)))
        );
    }

    public function test_time_from_string_with_definition() : void
    {
        self::assertEquals(
            time_entry('e', new \DateInterval('P10D')),
            (new EntryFactory())->create('e', 'P10D', schema(time_schema('e')))
        );
    }

    #[DataProvider('provide_unrecognized_data')]
    public function test_unrecognized_data_set_same_as_provided(string $input) : void
    {
        self::assertEquals(
            str_entry('e', $input),
            (new EntryFactory())->create('e', $input)
        );
    }

    public function test_uuid_from_ramsey_uuid_library() : void
    {
        if (!\class_exists(Uuid::class)) {
            self::markTestSkipped("Package 'ramsey/uuid' is required for this test.");
        }

        self::assertEquals(
            uuid_entry('e', $uuid = Uuid::uuid4()->toString()),
            (new EntryFactory())->create('e', $uuid)
        );
    }

    public function test_uuid_from_string() : void
    {
        self::assertEquals(
            uuid_entry('e', $uuid = '00000000-0000-0000-0000-000000000000'),
            (new EntryFactory())->create('e', $uuid)
        );
    }

    public function test_uuid_string_with_uuid_definition_provided() : void
    {
        self::assertEquals(
            uuid_entry('e', $uuid = '00000000-0000-0000-0000-000000000000'),
            (new EntryFactory())->create('e', $uuid, schema(uuid_schema('e')))
        );
    }

    public function test_uuid_type() : void
    {
        self::assertEquals(
            uuid_entry('e', '00000000-0000-0000-0000-000000000000'),
            (new EntryFactory())->create('e', '00000000-0000-0000-0000-000000000000')
        );
    }

    public function test_with_empty_schema() : void
    {
        $this->expectException(SchemaDefinitionNotFoundException::class);

        (new EntryFactory())
            ->create('e', '1', empty_schema());
    }

    public function test_with_schema_for_different_entry() : void
    {
        $this->expectException(SchemaDefinitionNotFoundException::class);

        (new EntryFactory())
            ->create('diff', '1', schema(string_schema('e')));
    }

    public function test_xml_from_dom_document() : void
    {
        $doc = new \DOMDocument();
        $doc->loadXML($xml = '<root><foo>1</foo><bar>2</bar><baz>3</baz></root>');
        self::assertEquals(
            xml_entry('e', $xml),
            (new EntryFactory())->create('e', $doc)
        );
    }

    public function test_xml_from_string() : void
    {
        self::assertEquals(
            xml_entry('e', $xml = '<root><foo>1</foo><bar>2</bar><baz>3</baz></root>'),
            (new EntryFactory())->create('e', $xml)
        );
    }

    public function test_xml_string_with_xml_definition_provided() : void
    {
        self::assertEquals(
            xml_entry('e', $xml = '<root><foo>1</foo><bar>2</bar><baz>3</baz></root>'),
            (new EntryFactory())->create('e', $xml, schema(xml_schema('e')))
        );
    }
}
