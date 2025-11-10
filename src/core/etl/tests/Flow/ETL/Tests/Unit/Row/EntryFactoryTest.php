<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row;

use function Flow\ETL\DSL\{bool_entry, bool_schema, config, date_entry, date_schema, datetime_entry, datetime_schema, enum_entry, enum_schema, float_entry, float_schema, flow_context, html_element_entry, html_element_schema, html_entry, html_schema, int_entry, integer_schema, json_entry, json_object_entry, json_schema, list_entry, list_schema, schema, str_entry, string_entry, string_schema, structure_entry, time_entry, time_schema, uuid_entry, uuid_schema, xml_entry, xml_schema};
use function Flow\Types\DSL\{type_datetime, type_float, type_integer, type_list, type_map, type_null, type_string, type_structure, type_time_zone};
use Dom\HTMLDocument;
use Flow\ETL\Exception\{InvalidArgumentException, SchemaDefinitionNotFoundException};
use Flow\ETL\Row\Entry\{StringEntry, TimeEntry};
use Flow\ETL\Row\{Entry, EntryFactory};
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Tests\Fixtures\Enum\BackedIntEnum;
use PHPUnit\Framework\Attributes\{DataProvider, RequiresPhp};
use PHPUnit\Framework\TestCase;
use Ramsey\Uuid\Uuid;

final class EntryFactoryTest extends TestCase
{
    private EntryFactory $entryFactory;

    public static function provide_recognized_data() : \Generator
    {
        yield 'json' => [
            $json = '{"id":1}',
            json_entry('e', $json),
        ];

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

        yield 'html alike' => [
            '<html',
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

    protected function setUp() : void
    {
        $this->entryFactory = flow_context(config())->entryFactory();
    }

    public function test_array_structure() : void
    {
        self::assertEquals(
            structure_entry('e', ['a' => 1, 'b' => '2'], type_structure(['a' => type_integer(), 'b' => type_string()])),
            $this->entryFactory->create('e', ['a' => 1, 'b' => '2'])
        );
    }

    public function test_bool() : void
    {
        self::assertEquals(
            bool_entry('e', false),
            $this->entryFactory->create('e', false)
        );
    }

    public function test_boolean_with_schema() : void
    {
        self::assertEquals(
            bool_entry('e', false),
            $this->entryFactory->create('e', false, schema(bool_schema('e')))
        );
    }

    public function test_date() : void
    {
        self::assertEquals(
            date_entry('e', '2022-01-01'),
            $this->entryFactory->create('e', new \DateTimeImmutable('2022-01-01'))
        );
    }

    public function test_date_from_int_with_definition() : void
    {
        self::assertEquals(
            date_entry('e', '1970-01-01'),
            $this->entryFactory->create('e', 1, schema(date_schema('e')))
        );
    }

    public function test_date_from_null_with_definition() : void
    {
        self::assertEquals(
            date_entry('e', null),
            $this->entryFactory->create('e', null, schema(date_schema('e', true)))
        );
    }

    public function test_date_from_string_with_definition() : void
    {
        self::assertEquals(
            date_entry('e', '2022-01-01'),
            $this->entryFactory->create('e', '2022-01-01', schema(date_schema('e')))
        );
    }

    public function test_datetime() : void
    {
        self::assertEquals(
            datetime_entry('e', $now = new \DateTimeImmutable()),
            $this->entryFactory->create('e', $now)
        );
    }

    public function test_datetime_string_with_schema() : void
    {
        self::assertEquals(
            datetime_entry('e', '2022-01-01 00:00:00 UTC'),
            $this->entryFactory
                ->create('e', '2022-01-01 00:00:00 UTC', schema(datetime_schema('e')))
        );
    }

    public function test_datetime_with_schema() : void
    {
        self::assertEquals(
            datetime_entry('e', $datetime = new \DateTimeImmutable('now')),
            $this->entryFactory
                ->create('e', $datetime, schema(datetime_schema('e')))
        );
    }

    public function test_enum() : void
    {
        self::assertEquals(
            enum_entry('e', $enum = BackedIntEnum::one),
            $this->entryFactory
                ->create('e', $enum)
        );
    }

    public function test_enum_from_string_with_schema() : void
    {
        self::assertEquals(
            enum_entry('e', BackedIntEnum::one),
            $this->entryFactory
                ->create('e', 1, schema(enum_schema('e', BackedIntEnum::class)))
        );
    }

    public function test_enum_invalid_value_with_schema() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Entry "e" conversion exception. Can\'t cast "string" into "enum<Flow\ETL\Tests\Fixtures\Enum\BackedIntEnum>" type');

        $this->entryFactory
            ->create('e', 'invalid', schema(enum_schema('e', BackedIntEnum::class)));
    }

    public function test_float() : void
    {
        self::assertEquals(
            float_entry('e', 1.1),
            $this->entryFactory->create('e', 1.1)
        );
    }

    public function test_float_with_schema() : void
    {
        self::assertEquals(
            float_entry('e', 1.1),
            $this->entryFactory->create('e', 1.1, schema(float_schema('e')))
        );
    }

    public function test_float_with_schema_and_metadata() : void
    {
        self::assertEquals(
            float_entry('e', 1.1, metadata: Metadata::with('test', 1)),
            $this->entryFactory->create('e', 1.1, schema(float_schema('e', metadata: Metadata::with('test', 1))))
        );
    }

    public function test_from_empty_string() : void
    {
        self::assertEquals(
            str_entry('e', ''),
            $this->entryFactory->create('e', '')
        );
    }

    public function test_html_element_from_string() : void
    {
        self::assertEquals(
            string_entry('e', $html = '<div>2</div><p>bar</p>'),
            $this->entryFactory->create('e', $html)
        );
    }

    #[RequiresPhp('>= 8.4')]
    public function test_html_element_string_with_html_definition_provided() : void
    {
        self::assertEquals(
            html_element_entry('e', $html = '<div>2</div><p>bar</p>'),
            $this->entryFactory->create('e', $html, schema(html_element_schema('e')))
        );
    }

    #[RequiresPhp('>= 8.4')]
    public function test_html_from_dom_html_document() : void
    {
        /* @phpstan-ignore-next-line */
        $doc = HTMLDocument::createFromString($html = '<!DOCTYPE html><html lang="en"><head></head><body><div>2</div><p>3</p></body></html>');

        self::assertEquals(
            html_entry('e', $html),
            $this->entryFactory->create('e', $doc)
        );
    }

    public function test_html_from_string() : void
    {
        self::assertEquals(
            string_entry('e', $html = '<!DOCTYPE html><html lang="en"><head></head><body><div>foo</div><p>3</p></body></html>'),
            $this->entryFactory->create('e', $html)
        );
    }

    #[RequiresPhp('>= 8.4')]
    public function test_html_string_with_html_definition_provided() : void
    {
        /* @phpstan-ignore-next-line */
        $document = HTMLDocument::createFromString(
            $html = '<!DOCTYPE html><html lang="en"><head></head><body><div>2</div><p>bar</p></body></html>'
        );

        self::assertEquals(
            html_entry('e', $html),
            $this->entryFactory->create('e', $document, schema(html_schema('e')))
        );
    }

    public function test_int() : void
    {
        self::assertEquals(
            int_entry('e', 1),
            $this->entryFactory->create('e', 1)
        );
    }

    public function test_integer_with_schema() : void
    {
        self::assertEquals(
            int_entry('e', 1),
            $this->entryFactory->create('e', 1, schema(integer_schema('e')))
        );
    }

    public function test_integer_with_schema_and_metadata() : void
    {
        self::assertEquals(
            int_entry('e', 1, metadata: Metadata::with('test', 1)),
            $this->entryFactory->create('e', 1, schema(integer_schema('e', metadata: Metadata::with('test', 1))))
        );
    }

    public function test_json() : void
    {
        self::assertEquals(
            json_entry('e', '{}'),
            $this->entryFactory->create('e', '{}')
        );
    }

    public function test_json_object() : void
    {
        self::assertEquals(
            json_object_entry('e', ['id' => 1]),
            $this->entryFactory->create('e', '{"id":1}')
        );
    }

    public function test_json_object_array_with_schema() : void
    {
        self::assertEquals(
            json_object_entry('e', ['id' => 1]),
            $this->entryFactory->create('e', ['id' => 1], schema(json_schema('e')))
        );
    }

    public function test_json_string() : void
    {
        self::assertEquals(
            json_entry('e', '{"id": 1}'),
            $this->entryFactory->create('e', '{"id": 1}')
        );
    }

    public function test_json_string_with_schema() : void
    {
        self::assertEquals(
            json_entry('e', '{"id": 1}'),
            $this->entryFactory->create('e', '{"id": 1}', schema(json_schema('e')))
        );
    }

    public function test_json_with_schema() : void
    {
        self::assertEquals(
            json_entry('e', [['id' => 1]]),
            $this->entryFactory->create('e', [['id' => 1]], schema(json_schema('e')))
        );
    }

    public function test_list_int_with_schema() : void
    {
        self::assertEquals(
            list_entry('e', [1, 2, 3], type_list(type_integer())),
            $this->entryFactory->create('e', [1, 2, 3], schema(list_schema('e', type_list(type_integer()))))
        );
    }

    public function test_list_int_with_schema_but_string_list() : void
    {
        self::assertEquals(
            list_entry('e', ['false', 'true', 'true'], type_list(type_string())),
            $this->entryFactory->create('e', [false, true, true], schema(list_schema('e', type_list(type_string()))))
        );
    }

    public function test_list_of_datetime_with_schema() : void
    {
        self::assertEquals(
            list_entry('e', $list = [new \DateTimeImmutable('now'), new \DateTimeImmutable('tomorrow')], type_list(type_datetime())),
            $this->entryFactory
                ->create('e', $list, schema(list_schema('e', type_list(type_datetime()))))
        );
    }

    public function test_list_of_datetimes() : void
    {
        self::assertEquals(
            list_entry('e', $list = [new \DateTimeImmutable(), new \DateTimeImmutable()], type_list(type_datetime())),
            $this->entryFactory->create('e', $list)
        );
    }

    public function test_list_of_scalars() : void
    {
        self::assertEquals(
            list_entry('e', [1, 2], type_list(type_integer())),
            $this->entryFactory->create('e', [1, 2])
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
            $this->entryFactory->create('address', [
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

    public function test_null_type_handled() : void
    {
        self::assertEquals(
            StringEntry::fromNull('e'),
            $this->entryFactory->createAs('e', null, type_null())
        );
    }

    public function test_object() : void
    {
        $this->expectExceptionMessage("e: object<ArrayIterator> can't be converted to any known Entry, please normalize that object first");

        $this->entryFactory->create('e', new \ArrayIterator([1, 2]));
    }

    /**
     * @param Entry<mixed> $entry
     */
    #[DataProvider('provide_recognized_data')]
    public function test_recognized_data_set_same_as_provided(string $input, Entry $entry) : void
    {
        self::assertEquals(
            $entry,
            $this->entryFactory->create('e', $input)
        );
    }

    public function test_string() : void
    {
        self::assertEquals(
            str_entry('e', 'test'),
            $this->entryFactory->create('e', 'test')
        );
    }

    public function test_string_with_schema() : void
    {
        self::assertEquals(
            str_entry('e', 'string'),
            $this->entryFactory->create('e', 'string', schema(string_schema('e')))
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
            $this->entryFactory->create('address', ['id' => 1, 'city' => 'Krakow', 'street' => 'Floriańska', 'zip' => '31-021'])
        );
    }

    public function test_time() : void
    {
        self::assertEquals(
            TimeEntry::fromDays('e', 1),
            $this->entryFactory->create('e', new \DateInterval('P1D'))
        );
    }

    public function test_time_from_null_with_definition() : void
    {
        self::assertEquals(
            time_entry('e', null),
            $this->entryFactory->create('e', null, schema(time_schema('e', true)))
        );
    }

    public function test_time_from_string_with_definition() : void
    {
        self::assertEquals(
            time_entry('e', new \DateInterval('P10D')),
            $this->entryFactory->create('e', 'P10D', schema(time_schema('e')))
        );
    }

    public function test_timezone_creates_string_entry() : void
    {
        self::assertEquals(
            str_entry('e', 'UTC'),
            $this->entryFactory->createAs('e', new \DateTimeZone('UTC'), type_time_zone())
        );
    }

    public function test_timezone_from_string_creates_string_entry() : void
    {
        self::assertEquals(
            str_entry('e', 'America/New_York'),
            $this->entryFactory->createAs('e', 'America/New_York', type_time_zone())
        );
    }

    #[DataProvider('provide_unrecognized_data')]
    public function test_unrecognized_data_set_same_as_provided(string $input) : void
    {
        self::assertEquals(
            str_entry('e', $input),
            $this->entryFactory->create('e', $input)
        );
    }

    public function test_uuid_from_ramsey_uuid_library() : void
    {
        if (!\class_exists(Uuid::class)) {
            self::markTestSkipped("Package 'ramsey/uuid' is required for this test.");
        }

        $uuidObject = Uuid::uuid4();
        self::assertEquals(
            uuid_entry('e', $uuidObject->toString()),
            $this->entryFactory->create('e', $uuidObject)
        );
    }

    public function test_uuid_from_string() : void
    {
        self::assertEquals(
            string_entry('e', $uuid = '00000000-0000-0000-0000-000000000000'),
            $this->entryFactory->create('e', $uuid)
        );
    }

    public function test_uuid_string_with_uuid_definition_provided() : void
    {
        self::assertEquals(
            uuid_entry('e', $uuid = '00000000-0000-0000-0000-000000000000'),
            $this->entryFactory->create('e', $uuid, schema(uuid_schema('e')))
        );
    }

    public function test_uuid_type() : void
    {
        self::assertEquals(
            string_entry('e', '00000000-0000-0000-0000-000000000000'),
            $this->entryFactory->create('e', '00000000-0000-0000-0000-000000000000')
        );
    }

    public function test_with_empty_schema() : void
    {
        $this->expectException(SchemaDefinitionNotFoundException::class);

        $this->entryFactory
            ->create('e', '1', schema());
    }

    public function test_with_schema_for_different_entry() : void
    {
        $this->expectException(SchemaDefinitionNotFoundException::class);

        $this->entryFactory
            ->create('diff', '1', schema(string_schema('e')));
    }

    public function test_xml_from_dom_document() : void
    {
        $doc = new \DOMDocument();
        $doc->loadXML($xml = '<root><foo>1</foo><bar>2</bar><baz>3</baz></root>');
        self::assertEquals(
            xml_entry('e', $xml),
            $this->entryFactory->create('e', $doc)
        );
    }

    public function test_xml_from_string() : void
    {
        self::assertEquals(
            string_entry('e', $xml = '<root><foo>1</foo><bar>2</bar><baz>3</baz></root>'),
            $this->entryFactory->create('e', $xml)
        );
    }

    public function test_xml_string_with_xml_definition_provided() : void
    {
        self::assertEquals(
            xml_entry('e', $xml = '<root><foo>1</foo><bar>2</bar><baz>3</baz></root>'),
            $this->entryFactory->create('e', $xml, schema(xml_schema('e')))
        );
    }
}
