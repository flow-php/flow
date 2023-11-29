<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Factory;

use function Flow\ETL\DSL\array_entry;
use function Flow\ETL\DSL\bool_entry;
use function Flow\ETL\DSL\datetime_entry;
use function Flow\ETL\DSL\enum_entry;
use function Flow\ETL\DSL\float_entry;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\json_entry;
use function Flow\ETL\DSL\json_object_entry;
use function Flow\ETL\DSL\list_entry;
use function Flow\ETL\DSL\null_entry;
use function Flow\ETL\DSL\object_entry;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\type_float;
use function Flow\ETL\DSL\type_int;
use function Flow\ETL\DSL\type_list;
use function Flow\ETL\DSL\type_object;
use function Flow\ETL\DSL\type_string;
use function Flow\ETL\DSL\uuid_entry;
use function Flow\ETL\DSL\xml_entry;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Logical\List\ListElement;
use Flow\ETL\PHP\Type\Logical\ListType;
use Flow\ETL\PHP\Type\Logical\Structure\StructureElement;
use Flow\ETL\PHP\Type\Logical\StructureType;
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
        $this->assertEquals(
            new StructureEntry(
                'e',
                ['a' => 1, 'b' => '2'],
                new StructureType(new StructureElement('a', type_int()), new StructureElement('b', type_string()))
            ),
            (new NativeEntryFactory())->create('e', ['a' => 1, 'b' => '2'])
        );
    }

    public function test_array_with_schema() : void
    {
        $this->assertEquals(
            array_entry('e', [1, 2, 3]),
            (new NativeEntryFactory())
                ->create('e', [1, 2, 3], new Schema(Schema\Definition::array('e')))
        );
    }

    public function test_bool() : void
    {
        $this->assertEquals(
            bool_entry('e', false),
            (new NativeEntryFactory())->create('e', false)
        );
    }

    public function test_boolean_with_schema() : void
    {
        $this->assertEquals(
            bool_entry('e', false),
            (new NativeEntryFactory())->create('e', false, new Schema(Schema\Definition::boolean('e')))
        );
    }

    public function test_conversion_to_different_type_with_schema() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Field \"e\" conversion exception. Flow\ETL\DSL\string_entry(): Argument #2 (\$value) must be of type string, int given, called in");

        (new NativeEntryFactory())
            ->create('e', 1, new Schema(Schema\Definition::string('e')));
    }

    public function test_datetime() : void
    {
        $this->assertEquals(
            datetime_entry('e', $now = new \DateTimeImmutable()),
            (new NativeEntryFactory())->create('e', $now)
        );
    }

    public function test_datetime_string_with_schema() : void
    {
        $this->assertEquals(
            datetime_entry('e', '2022-01-01 00:00:00 UTC'),
            (new NativeEntryFactory())
                ->create('e', '2022-01-01 00:00:00 UTC', new Schema(Schema\Definition::dateTime('e')))
        );
    }

    public function test_datetime_with_schema() : void
    {
        $this->assertEquals(
            datetime_entry('e', $datetime = new \DateTimeImmutable('now')),
            (new NativeEntryFactory())
                ->create('e', $datetime, new Schema(Schema\Definition::dateTime('e')))
        );
    }

    public function test_enum() : void
    {
        $this->assertEquals(
            enum_entry('e', $enum = BackedIntEnum::one),
            (new NativeEntryFactory())
                ->create('e', $enum)
        );
    }

    public function test_enum_from_string_with_schema() : void
    {
        $this->assertEquals(
            enum_entry('e', BackedIntEnum::one),
            (new NativeEntryFactory())
                ->create('e', 'one', new Schema(Schema\Definition::enum('e', BackedIntEnum::class)))
        );
    }

    public function test_enum_invalid_value_with_schema() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Value \"invalid\" can't be converted to " . BackedIntEnum::class . ' enum');

        (new NativeEntryFactory())
            ->create('e', 'invalid', new Schema(Schema\Definition::enum('e', BackedIntEnum::class)));
    }

    public function test_float() : void
    {
        $this->assertEquals(
            float_entry('e', 1.1),
            (new NativeEntryFactory())->create('e', 1.1)
        );
    }

    public function test_float_with_schema() : void
    {
        $this->assertEquals(
            float_entry('e', 1.1),
            (new NativeEntryFactory())->create('e', 1.1, new Schema(Schema\Definition::float('e')))
        );
    }

    public function test_from_empty_string() : void
    {
        $this->assertEquals(
            str_entry('e', ''),
            (new NativeEntryFactory())->create('e', '')
        );
    }

    public function test_int() : void
    {
        $this->assertEquals(
            int_entry('e', 1),
            (new NativeEntryFactory())->create('e', 1)
        );
    }

    public function test_integer_with_schema() : void
    {
        $this->assertEquals(
            int_entry('e', 1),
            (new NativeEntryFactory())->create('e', 1, new Schema(Schema\Definition::integer('e')))
        );
    }

    public function test_json() : void
    {
        $this->assertEquals(
            json_object_entry('e', ['id' => 1]),
            (new NativeEntryFactory())->create('e', '{"id":1}')
        );
    }

    public function test_json_object_array_with_schema() : void
    {
        $this->assertEquals(
            json_object_entry('e', ['id' => 1]),
            (new NativeEntryFactory())->create('e', ['id' => 1], new Schema(Schema\Definition::json('e')))
        );
    }

    public function test_json_string() : void
    {
        $this->assertEquals(
            json_entry('e', '{"id": 1}'),
            (new NativeEntryFactory())->create('e', '{"id": 1}')
        );
    }

    public function test_json_string_with_schema() : void
    {
        $this->assertEquals(
            json_entry('e', '{"id": 1}'),
            (new NativeEntryFactory())->create('e', '{"id": 1}', new Schema(Schema\Definition::json('e')))
        );
    }

    public function test_json_with_schema() : void
    {
        $this->assertEquals(
            json_entry('e', [['id' => 1]]),
            (new NativeEntryFactory())->create('e', [['id' => 1]], new Schema(Schema\Definition::json('e')))
        );
    }

    public function test_list_int_with_schema() : void
    {
        $this->assertEquals(
            list_entry('e', [1, 2, 3], type_list(type_int())),
            (new NativeEntryFactory())->create('e', [1, 2, 3], new Schema(Schema\Definition::list('e', new ListType(ListElement::integer()))))
        );
    }

    public function test_list_int_with_schema_but_string_list() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Field "e" conversion exception. Expected list<integer> got different types: list<string>');

        (new NativeEntryFactory())->create('e', ['1', '2', '3'], new Schema(Schema\Definition::list('e', new ListType(ListElement::integer()))));
    }

    public function test_list_of_datetime_with_schema() : void
    {
        $this->assertEquals(
            list_entry('e', $list = [new \DateTimeImmutable('now'), new \DateTimeImmutable('tomorrow')], type_list(type_object(\DateTimeImmutable::class))),
            (new NativeEntryFactory())
                ->create('e', $list, new Schema(Schema\Definition::list('e', new ListType(ListElement::object(\DateTimeImmutable::class)))))
        );
    }

    public function test_list_of_datetimes() : void
    {
        $this->assertEquals(
            list_entry('e', $list = [new \DateTimeImmutable(), new \DateTimeImmutable()], type_list(type_object(\DateTimeImmutable::class))),
            (new NativeEntryFactory())->create('e', $list)
        );
    }

    public function test_list_of_scalars() : void
    {
        $this->assertEquals(
            list_entry('e', [1, 2], type_list(type_int())),
            (new NativeEntryFactory())->create('e', [1, 2])
        );
    }

    public function test_list_of_string_datetime_with_schema() : void
    {
        $this->assertEquals(
            list_entry('e', [new \DateTimeImmutable('2022-01-01 00:00:00 UTC'), new \DateTimeImmutable('2022-01-01 00:00:00 UTC')], type_list(type_object(\DateTimeImmutable::class))),
            (new NativeEntryFactory())
                ->create(
                    'e',
                    ['2022-01-01 00:00:00 UTC', '2022-01-01 00:00:00 UTC'],
                    new Schema(Schema\Definition::list('e', new ListType(ListElement::object(\DateTimeImmutable::class))))
                )
        );
    }

    public function test_nested_structure() : void
    {
        $this->assertEquals(
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
                new StructureType(
                    new StructureElement('city', type_string()),
                    new StructureElement(
                        'geo',
                        new StructureType(
                            new StructureElement('lat', type_float()),
                            new StructureElement('lon', type_float())
                        ),
                    ),
                    new StructureElement('street', type_string()),
                    new StructureElement('zip', type_string()),
                ),
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

    public function test_null() : void
    {
        $this->assertEquals(
            null_entry('e'),
            (new NativeEntryFactory())->create('e', null)
        );
    }

    public function test_null_with_schema() : void
    {
        $this->assertEquals(
            null_entry('e'),
            (new NativeEntryFactory())->create('e', null, new Schema(Schema\Definition::null('e')))
        );

        $this->assertEquals(
            null_entry('e'),
            (new NativeEntryFactory())->create('e', null, new Schema(Schema\Definition::string('e', true)))
        );
    }

    public function test_object() : void
    {
        $this->assertEquals(
            object_entry('e', $object = new \ArrayIterator([1, 2])),
            (new NativeEntryFactory())->create('e', $object)
        );
    }

    public function test_object_with_schema() : void
    {
        $this->assertEquals(
            object_entry('e', $object = new \ArrayObject([1, 2, 3])),
            (new NativeEntryFactory())
                ->create('e', $object, new Schema(Schema\Definition::object('e', type_object($object::class))))
        );
    }

    public function test_string() : void
    {
        $this->assertEquals(
            str_entry('e', 'test'),
            (new NativeEntryFactory())->create('e', 'test')
        );
    }

    public function test_string_with_schema() : void
    {
        $this->assertEquals(
            str_entry('e', 'string'),
            (new NativeEntryFactory())->create('e', 'string', new Schema(Schema\Definition::string('e')))
        );
    }

    public function test_structure() : void
    {
        $this->assertEquals(
            new StructureEntry(
                'address',
                ['id' => 1, 'city' => 'Krakow', 'street' => 'Floriańska', 'zip' => '31-021'],
                new StructureType(
                    new StructureElement('id', type_int()),
                    new StructureElement('city', type_string()),
                    new StructureElement('street', type_string()),
                    new StructureElement('zip', type_string())
                )
            ),
            (new NativeEntryFactory())->create('address', ['id' => 1, 'city' => 'Krakow', 'street' => 'Floriańska', 'zip' => '31-021'])
        );
    }

    #[DataProvider('provide_unrecognized_data')]
    public function test_unrecognized_data_set_same_as_provided(string $input) : void
    {
        $this->assertEquals(
            str_entry('e', $input),
            (new NativeEntryFactory())->create('e', $input)
        );
    }

    public function test_uuid_from_ramsey_uuid_library() : void
    {
        if (!\class_exists(Uuid::class)) {
            $this->markTestSkipped("Package 'ramsey/uuid' is required for this test.");
        }

        $this->assertEquals(
            uuid_entry('e', $uuid = Uuid::uuid4()->toString()),
            (new NativeEntryFactory())->create('e', $uuid)
        );
    }

    public function test_uuid_from_string() : void
    {
        $this->assertEquals(
            uuid_entry('e', $uuid = '00000000-0000-0000-0000-000000000000'),
            (new NativeEntryFactory())->create('e', $uuid)
        );
    }

    public function test_uuid_string_with_uuid_definition_provided() : void
    {
        $this->assertEquals(
            uuid_entry('e', $uuid = '00000000-0000-0000-0000-000000000000'),
            (new NativeEntryFactory())->create('e', $uuid, new Schema(Schema\Definition::uuid('e')))
        );
    }

    public function test_with_empty_schema() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('There is no definition for "e" in the schema.');

        (new NativeEntryFactory())
            ->create('e', '1', new Schema());
    }

    public function test_with_schema_for_different_entry() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('There is no definition for "diff" in the schema.');

        (new NativeEntryFactory())
            ->create('diff', '1', new Schema(Schema\Definition::string('e')));
    }

    public function test_xml_from_dom_document() : void
    {
        $doc = new \DOMDocument();
        $doc->loadXML($xml = '<root><foo>1</foo><bar>2</bar><baz>3</baz></root>');
        $this->assertEquals(
            xml_entry('e', $xml),
            (new NativeEntryFactory())->create('e', $doc)
        );
    }

    public function test_xml_from_string() : void
    {
        $this->assertEquals(
            xml_entry('e', $xml = '<root><foo>1</foo><bar>2</bar><baz>3</baz></root>'),
            (new NativeEntryFactory())->create('e', $xml)
        );
    }

    public function test_xml_string_with_xml_definition_provided() : void
    {
        $this->assertEquals(
            xml_entry('e', $xml = '<root><foo>1</foo><bar>2</bar><baz>3</baz></root>'),
            (new NativeEntryFactory())->create('e', $xml, new Schema(Schema\Definition::xml('e')))
        );
    }
}
