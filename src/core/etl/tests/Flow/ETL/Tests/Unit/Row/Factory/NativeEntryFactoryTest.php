<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Factory;

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
    type_datetime,
    type_float,
    type_int,
    type_list,
    type_map,
    type_string,
    uuid_entry,
    xml_entry};
use function Flow\ETL\DSL\{bool_schema, date_schema, datetime_schema, enum_schema, float_schema, integer_schema, json_schema, list_schema, schema, string_schema, structure_entry, time_schema, type_integer, type_structure, uuid_schema, xml_schema};
use Flow\ETL\Exception\{CastingException, SchemaDefinitionNotFoundException};
use Flow\ETL\Row\Entry\{TimeEntry};
use Flow\ETL\Row\Factory\NativeEntryFactory;
use Flow\ETL\Tests\Fixtures\Enum\BackedIntEnum;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\DataProvider;
use Ramsey\Uuid\Uuid;

final class NativeEntryFactoryTest extends FlowTestCase
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
            structure_entry('e', ['a' => 1, 'b' => '2'], type_structure(['a' => type_int(), 'b' => type_string()])),
            (new NativeEntryFactory())->create('e', ['a' => 1, 'b' => '2'])
        );
    }

    public function test_bool() : void
    {
        self::assertEquals(
            bool_entry('e', false),
            (new NativeEntryFactory())->create('e', false)
        );
    }

    public function test_boolean_with_schema() : void
    {
        self::assertEquals(
            bool_entry('e', false),
            (new NativeEntryFactory())->create('e', false, schema(bool_schema('e')))
        );
    }

    public function test_date() : void
    {
        self::assertEquals(
            date_entry('e', '2022-01-01'),
            (new NativeEntryFactory())->create('e', new \DateTimeImmutable('2022-01-01'))
        );
    }

    public function test_date_from_int_with_definition() : void
    {
        self::assertEquals(
            date_entry('e', '1970-01-01'),
            (new NativeEntryFactory())->create('e', 1, schema(date_schema('e')))
        );
    }

    public function test_date_from_null_with_definition() : void
    {
        self::assertEquals(
            date_entry('e', null),
            (new NativeEntryFactory())->create('e', null, schema(date_schema('e', true)))
        );
    }

    public function test_date_from_string_with_definition() : void
    {
        self::assertEquals(
            date_entry('e', '2022-01-01'),
            (new NativeEntryFactory())->create('e', '2022-01-01', schema(date_schema('e')))
        );
    }

    public function test_datetime() : void
    {
        self::assertEquals(
            datetime_entry('e', $now = new \DateTimeImmutable()),
            (new NativeEntryFactory())->create('e', $now)
        );
    }

    public function test_datetime_string_with_schema() : void
    {
        self::assertEquals(
            datetime_entry('e', '2022-01-01 00:00:00 UTC'),
            (new NativeEntryFactory())
                ->create('e', '2022-01-01 00:00:00 UTC', schema(datetime_schema('e')))
        );
    }

    public function test_datetime_with_schema() : void
    {
        self::assertEquals(
            datetime_entry('e', $datetime = new \DateTimeImmutable('now')),
            (new NativeEntryFactory())
                ->create('e', $datetime, schema(datetime_schema('e')))
        );
    }

    public function test_enum() : void
    {
        self::assertEquals(
            enum_entry('e', $enum = BackedIntEnum::one),
            (new NativeEntryFactory())
                ->create('e', $enum)
        );
    }

    public function test_enum_from_string_with_schema() : void
    {
        self::assertEquals(
            enum_entry('e', BackedIntEnum::one),
            (new NativeEntryFactory())
                ->create('e', 1, schema(enum_schema('e', BackedIntEnum::class)))
        );
    }

    public function test_enum_invalid_value_with_schema() : void
    {
        $this->expectException(CastingException::class);
        $this->expectExceptionMessage("Can't cast \"string\" into \"enum<Flow\ETL\Tests\Fixtures\Enum\BackedIntEnum>\" type");

        (new NativeEntryFactory())
            ->create('e', 'invalid', schema(enum_schema('e', BackedIntEnum::class)));
    }

    public function test_float() : void
    {
        self::assertEquals(
            float_entry('e', 1.1),
            (new NativeEntryFactory())->create('e', 1.1)
        );
    }

    public function test_float_with_schema() : void
    {
        self::assertEquals(
            float_entry('e', 1.1),
            (new NativeEntryFactory())->create('e', 1.1, schema(float_schema('e')))
        );
    }

    public function test_from_empty_string() : void
    {
        self::assertEquals(
            str_entry('e', ''),
            (new NativeEntryFactory())->create('e', '')
        );
    }

    public function test_int() : void
    {
        self::assertEquals(
            int_entry('e', 1),
            (new NativeEntryFactory())->create('e', 1)
        );
    }

    public function test_integer_with_schema() : void
    {
        self::assertEquals(
            int_entry('e', 1),
            (new NativeEntryFactory())->create('e', 1, schema(integer_schema('e')))
        );
    }

    public function test_json() : void
    {
        self::assertEquals(
            json_object_entry('e', ['id' => 1]),
            (new NativeEntryFactory())->create('e', '{"id":1}')
        );
    }

    public function test_json_object_array_with_schema() : void
    {
        self::assertEquals(
            json_object_entry('e', ['id' => 1]),
            (new NativeEntryFactory())->create('e', ['id' => 1], schema(json_schema('e')))
        );
    }

    public function test_json_string() : void
    {
        self::assertEquals(
            json_entry('e', '{"id": 1}'),
            (new NativeEntryFactory())->create('e', '{"id": 1}')
        );
    }

    public function test_json_string_with_schema() : void
    {
        self::assertEquals(
            json_entry('e', '{"id": 1}'),
            (new NativeEntryFactory())->create('e', '{"id": 1}', schema(json_schema('e')))
        );
    }

    public function test_json_with_schema() : void
    {
        self::assertEquals(
            json_entry('e', [['id' => 1]]),
            (new NativeEntryFactory())->create('e', [['id' => 1]], schema(json_schema('e')))
        );
    }

    public function test_list_int_with_schema() : void
    {
        self::assertEquals(
            list_entry('e', [1, 2, 3], type_list(type_int())),
            (new NativeEntryFactory())->create('e', [1, 2, 3], schema(list_schema('e', type_list(type_integer()))))
        );
    }

    public function test_list_int_with_schema_but_string_list() : void
    {
        self::assertEquals(
            list_entry('e', ['false', 'true', 'true'], type_list(type_string())),
            (new NativeEntryFactory())->create('e', [false, true, true], schema(list_schema('e', type_list(type_string()))))
        );
    }

    public function test_list_of_datetime_with_schema() : void
    {
        self::assertEquals(
            list_entry('e', $list = [new \DateTimeImmutable('now'), new \DateTimeImmutable('tomorrow')], type_list(type_datetime())),
            (new NativeEntryFactory())
                ->create('e', $list, schema(list_schema('e', type_list(type_datetime()))))
        );
    }

    public function test_list_of_datetimes() : void
    {
        self::assertEquals(
            list_entry('e', $list = [new \DateTimeImmutable(), new \DateTimeImmutable()], type_list(type_datetime())),
            (new NativeEntryFactory())->create('e', $list)
        );
    }

    public function test_list_of_scalars() : void
    {
        self::assertEquals(
            list_entry('e', [1, 2], type_list(type_int())),
            (new NativeEntryFactory())->create('e', [1, 2])
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
            (new NativeEntryFactory())->create('address', [
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

        (new NativeEntryFactory())->create('e', new \ArrayIterator([1, 2]));
    }

    public function test_string() : void
    {
        self::assertEquals(
            str_entry('e', 'test'),
            (new NativeEntryFactory())->create('e', 'test')
        );
    }

    public function test_string_with_schema() : void
    {
        self::assertEquals(
            str_entry('e', 'string'),
            (new NativeEntryFactory())->create('e', 'string', schema(string_schema('e')))
        );
    }

    public function test_structure() : void
    {
        self::assertEquals(
            structure_entry('address', ['id' => 1, 'city' => 'Krakow', 'street' => 'Floriańska', 'zip' => '31-021'], type_structure([
                'id' => type_int(),
                'city' => type_string(),
                'street' => type_string(),
                'zip' => type_string(),
            ])),
            (new NativeEntryFactory())->create('address', ['id' => 1, 'city' => 'Krakow', 'street' => 'Floriańska', 'zip' => '31-021'])
        );
    }

    public function test_time() : void
    {
        self::assertEquals(
            TimeEntry::fromDays('e', 1),
            (new NativeEntryFactory())->create('e', new \DateInterval('P1D'))
        );
    }

    public function test_time_from_null_with_definition() : void
    {
        self::assertEquals(
            time_entry('e', null),
            (new NativeEntryFactory())->create('e', null, schema(time_schema('e', true)))
        );
    }

    public function test_time_from_string_with_definition() : void
    {
        self::assertEquals(
            time_entry('e', new \DateInterval('P10D')),
            (new NativeEntryFactory())->create('e', 'P10D', schema(time_schema('e')))
        );
    }

    #[DataProvider('provide_unrecognized_data')]
    public function test_unrecognized_data_set_same_as_provided(string $input) : void
    {
        self::assertEquals(
            str_entry('e', $input),
            (new NativeEntryFactory())->create('e', $input)
        );
    }

    public function test_uuid_from_ramsey_uuid_library() : void
    {
        if (!\class_exists(Uuid::class)) {
            self::markTestSkipped("Package 'ramsey/uuid' is required for this test.");
        }

        self::assertEquals(
            uuid_entry('e', $uuid = Uuid::uuid4()->toString()),
            (new NativeEntryFactory())->create('e', $uuid)
        );
    }

    public function test_uuid_from_string() : void
    {
        self::assertEquals(
            uuid_entry('e', $uuid = '00000000-0000-0000-0000-000000000000'),
            (new NativeEntryFactory())->create('e', $uuid)
        );
    }

    public function test_uuid_string_with_uuid_definition_provided() : void
    {
        self::assertEquals(
            uuid_entry('e', $uuid = '00000000-0000-0000-0000-000000000000'),
            (new NativeEntryFactory())->create('e', $uuid, schema(uuid_schema('e')))
        );
    }

    public function test_with_empty_schema() : void
    {
        $this->expectException(SchemaDefinitionNotFoundException::class);

        (new NativeEntryFactory())
            ->create('e', '1', schema());
    }

    public function test_with_schema_for_different_entry() : void
    {
        $this->expectException(SchemaDefinitionNotFoundException::class);

        (new NativeEntryFactory())
            ->create('diff', '1', schema(string_schema('e')));
    }

    public function test_xml_from_dom_document() : void
    {
        $doc = new \DOMDocument();
        $doc->loadXML($xml = '<root><foo>1</foo><bar>2</bar><baz>3</baz></root>');
        self::assertEquals(
            xml_entry('e', $xml),
            (new NativeEntryFactory())->create('e', $doc)
        );
    }

    public function test_xml_from_string() : void
    {
        self::assertEquals(
            xml_entry('e', $xml = '<root><foo>1</foo><bar>2</bar><baz>3</baz></root>'),
            (new NativeEntryFactory())->create('e', $xml)
        );
    }

    public function test_xml_string_with_xml_definition_provided() : void
    {
        self::assertEquals(
            xml_entry('e', $xml = '<root><foo>1</foo><bar>2</bar><baz>3</baz></root>'),
            (new NativeEntryFactory())->create('e', $xml, schema(xml_schema('e')))
        );
    }
}
