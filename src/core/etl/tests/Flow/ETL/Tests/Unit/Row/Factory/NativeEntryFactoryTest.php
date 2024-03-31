<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Factory;

use function Flow\ETL\DSL\{array_entry, bool_entry, datetime_entry, enum_entry, float_entry, int_entry, json_entry, json_object_entry, list_entry, object_entry, str_entry, type_datetime, type_float, type_int, type_list, type_object, type_string, uuid_entry, xml_entry};
use Flow\ETL\Exception\{CastingException, SchemaDefinitionNotFoundException};
use Flow\ETL\PHP\Type\Logical\List\ListElement;
use Flow\ETL\PHP\Type\Logical\Structure\StructureElement;
use Flow\ETL\PHP\Type\Logical\{ListType, StructureType};
use Flow\ETL\Row\Entry\StructureEntry;
use Flow\ETL\Row\Factory\NativeEntryFactory;
use Flow\ETL\Row\Schema;
use Flow\ETL\Tests\Fixtures\Enum\BackedIntEnum;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Ramsey\Uuid\Uuid;

final class NativeEntryFactoryTest extends TestCase
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
            new StructureEntry(
                'e',
                ['a' => 1, 'b' => '2'],
                new StructureType([new StructureElement('a', type_int()), new StructureElement('b', type_string())])
            ),
            (new NativeEntryFactory())->create('e', ['a' => 1, 'b' => '2'])
        );
    }

    public function test_array_with_schema() : void
    {
        self::assertEquals(
            array_entry('e', [1, 2, 3]),
            (new NativeEntryFactory())
                ->create('e', [1, 2, 3], new Schema(Schema\Definition::array('e')))
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
            (new NativeEntryFactory())->create('e', false, new Schema(Schema\Definition::boolean('e')))
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
                ->create('e', '2022-01-01 00:00:00 UTC', new Schema(Schema\Definition::dateTime('e')))
        );
    }

    public function test_datetime_with_schema() : void
    {
        self::assertEquals(
            datetime_entry('e', $datetime = new \DateTimeImmutable('now')),
            (new NativeEntryFactory())
                ->create('e', $datetime, new Schema(Schema\Definition::dateTime('e')))
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
                ->create('e', 1, new Schema(Schema\Definition::enum('e', BackedIntEnum::class)))
        );
    }

    public function test_enum_invalid_value_with_schema() : void
    {
        $this->expectException(CastingException::class);
        $this->expectExceptionMessage("Can't cast \"string\" into \"enum<Flow\ETL\Tests\Fixtures\Enum\BackedIntEnum>\" type");

        (new NativeEntryFactory())
            ->create('e', 'invalid', new Schema(Schema\Definition::enum('e', BackedIntEnum::class)));
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
            (new NativeEntryFactory())->create('e', 1.1, new Schema(Schema\Definition::float('e')))
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
            (new NativeEntryFactory())->create('e', 1, new Schema(Schema\Definition::integer('e')))
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
            (new NativeEntryFactory())->create('e', ['id' => 1], new Schema(Schema\Definition::json('e')))
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
            (new NativeEntryFactory())->create('e', '{"id": 1}', new Schema(Schema\Definition::json('e')))
        );
    }

    public function test_json_with_schema() : void
    {
        self::assertEquals(
            json_entry('e', [['id' => 1]]),
            (new NativeEntryFactory())->create('e', [['id' => 1]], new Schema(Schema\Definition::json('e')))
        );
    }

    public function test_list_int_with_schema() : void
    {
        self::assertEquals(
            list_entry('e', [1, 2, 3], type_list(type_int())),
            (new NativeEntryFactory())->create('e', [1, 2, 3], new Schema(Schema\Definition::list('e', new ListType(ListElement::integer()))))
        );
    }

    public function test_list_int_with_schema_but_string_list() : void
    {
        self::assertEquals(
            list_entry('e', ['false', 'true', 'true'], type_list(type_string())),
            (new NativeEntryFactory())->create('e', [false, true, true], new Schema(Schema\Definition::list('e', new ListType(ListElement::string()))))
        );
    }

    public function test_list_of_datetime_with_schema() : void
    {
        self::assertEquals(
            list_entry('e', $list = [new \DateTimeImmutable('now'), new \DateTimeImmutable('tomorrow')], type_list(type_datetime())),
            (new NativeEntryFactory())
                ->create('e', $list, new Schema(Schema\Definition::list('e', new ListType(ListElement::datetime()))))
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
            new StructureEntry(
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
                new StructureType([
                    new StructureElement('city', type_string()),
                    new StructureElement(
                        'geo',
                        new StructureType([
                            new StructureElement('lat', type_float()),
                            new StructureElement('lon', type_float()),
                        ]),
                    ),
                    new StructureElement('street', type_string()),
                    new StructureElement('zip', type_string()),
                ]),
            ),
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
        self::assertEquals(
            object_entry('e', $object = new \ArrayIterator([1, 2])),
            (new NativeEntryFactory())->create('e', $object)
        );
    }

    public function test_object_with_schema() : void
    {
        self::assertEquals(
            object_entry('e', $object = new \ArrayObject([1, 2, 3])),
            (new NativeEntryFactory())
                ->create('e', $object, new Schema(Schema\Definition::object('e', type_object($object::class))))
        );
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
            (new NativeEntryFactory())->create('e', 'string', new Schema(Schema\Definition::string('e')))
        );
    }

    public function test_structure() : void
    {
        self::assertEquals(
            new StructureEntry(
                'address',
                ['id' => 1, 'city' => 'Krakow', 'street' => 'Floriańska', 'zip' => '31-021'],
                new StructureType([
                    new StructureElement('id', type_int()),
                    new StructureElement('city', type_string()),
                    new StructureElement('street', type_string()),
                    new StructureElement('zip', type_string()),
                ])
            ),
            (new NativeEntryFactory())->create('address', ['id' => 1, 'city' => 'Krakow', 'street' => 'Floriańska', 'zip' => '31-021'])
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
            (new NativeEntryFactory())->create('e', $uuid, new Schema(Schema\Definition::uuid('e')))
        );
    }

    public function test_with_empty_schema() : void
    {
        $this->expectException(SchemaDefinitionNotFoundException::class);

        (new NativeEntryFactory())
            ->create('e', '1', new Schema());
    }

    public function test_with_schema_for_different_entry() : void
    {
        $this->expectException(SchemaDefinitionNotFoundException::class);

        (new NativeEntryFactory())
            ->create('diff', '1', new Schema(Schema\Definition::string('e')));
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
            (new NativeEntryFactory())->create('e', $xml, new Schema(Schema\Definition::xml('e')))
        );
    }
}
