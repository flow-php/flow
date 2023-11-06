<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Factory;

use Flow\ETL\DSL\Entry;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\ObjectType;
use Flow\ETL\PHP\Type\ScalarType;
use Flow\ETL\Row\Factory\NativeEntryFactory;
use Flow\ETL\Row\Schema;
use Flow\ETL\Tests\Fixtures\Enum\BackedIntEnum;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

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

    public function test_array() : void
    {
        $this->assertEquals(
            Entry::structure('e', Entry::int('a', 1), Entry::int('b', 2)),
            (new NativeEntryFactory())->create('e', ['a' => 1, 'b' => 2])
        );
    }

    public function test_array_with_schema() : void
    {
        $this->assertEquals(
            Entry::array('e', [1, 2, 3]),
            (new NativeEntryFactory())
                ->create('e', [1, 2, 3], new Schema(Schema\Definition::array('e')))
        );
    }

    public function test_bool() : void
    {
        $this->assertEquals(
            Entry::boolean('e', false),
            (new NativeEntryFactory())->create('e', false)
        );
    }

    public function test_boolean_with_schema() : void
    {
        $this->assertEquals(
            Entry::boolean('e', false),
            (new NativeEntryFactory())->create('e', false, new Schema(Schema\Definition::boolean('e')))
        );
    }

    public function test_conversion_to_different_type_with_schema() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Can't convert value into entry \"e\"");

        (new NativeEntryFactory())
            ->create('e', 1, new Schema(Schema\Definition::string('e')));
    }

    public function test_datetime() : void
    {
        $this->assertEquals(
            Entry::datetime('e', $now = new \DateTimeImmutable()),
            (new NativeEntryFactory())->create('e', $now)
        );
    }

    public function test_datetime_string_with_schema() : void
    {
        $this->assertEquals(
            Entry::datetime_string('e', '2022-01-01 00:00:00 UTC'),
            (new NativeEntryFactory())
                ->create('e', '2022-01-01 00:00:00 UTC', new Schema(Schema\Definition::dateTime('e')))
        );
    }

    public function test_datetime_with_schema() : void
    {
        $this->assertEquals(
            Entry::datetime('e', $datetime = new \DateTimeImmutable('now')),
            (new NativeEntryFactory())
                ->create('e', $datetime, new Schema(Schema\Definition::dateTime('e')))
        );
    }

    public function test_enum_invalid_value_with_schema() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Value \"not_valid\" can't be converted to " . BackedIntEnum::class . ' enum');

        (new NativeEntryFactory())
            ->create('e', 'not_valid', new Schema(Schema\Definition::enum('e', BackedIntEnum::class)));
    }

    public function test_enum_with_schema() : void
    {
        $this->assertEquals(
            Entry::enum('e', BackedIntEnum::one),
            (new NativeEntryFactory())
                ->create('e', 'one', new Schema(Schema\Definition::enum('e', BackedIntEnum::class)))
        );
    }

    public function test_float() : void
    {
        $this->assertEquals(
            Entry::float('e', 1.1),
            (new NativeEntryFactory())->create('e', 1.1)
        );
    }

    public function test_float_with_schema() : void
    {
        $this->assertEquals(
            Entry::float('e', 1.1),
            (new NativeEntryFactory())->create('e', 1.1, new Schema(Schema\Definition::float('e')))
        );
    }

    public function test_from_empty_string() : void
    {
        $this->assertEquals(
            Entry::string('e', ''),
            (new NativeEntryFactory())->create('e', '')
        );
    }

    public function test_int() : void
    {
        $this->assertEquals(
            Entry::integer('e', 1),
            (new NativeEntryFactory())->create('e', 1)
        );
    }

    public function test_integer_with_schema() : void
    {
        $this->assertEquals(
            Entry::integer('e', 1),
            (new NativeEntryFactory())->create('e', 1, new Schema(Schema\Definition::integer('e')))
        );
    }

    public function test_json() : void
    {
        $this->assertEquals(
            Entry::json_object('e', ['id' => 1]),
            (new NativeEntryFactory())->create('e', '{"id":1}')
        );
    }

    public function test_json_array_with_schema() : void
    {
        $this->assertEquals(
            Entry::json('e', [['id' => 1]]),
            (new NativeEntryFactory())->create('e', [['id' => 1]], new Schema(Schema\Definition::json('e')))
        );
    }

    public function test_json_object_array_with_schema() : void
    {
        $this->assertEquals(
            Entry::json_object('e', ['id' => 1]),
            (new NativeEntryFactory())->create('e', ['id' => 1], new Schema(Schema\Definition::json('e')))
        );
    }

    public function test_json_string_with_schema() : void
    {
        $this->assertEquals(
            Entry::json_string('e', '{"id": 1}'),
            (new NativeEntryFactory())->create('e', '{"id": 1}', new Schema(Schema\Definition::json('e')))
        );
    }

    public function test_list_int_with_schema() : void
    {
        $this->assertEquals(
            Entry::list_of_int('e', [1, 2, 3]),
            (new NativeEntryFactory())->create('e', [1, 2, 3], new Schema(Schema\Definition::list('e', ScalarType::integer)))
        );
    }

    public function test_list_int_with_schema_but_string_list() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Field "e" conversion exception. Expected list of integer got different types.');

        (new NativeEntryFactory())->create('e', ['1', '2', '3'], new Schema(Schema\Definition::list('e', ScalarType::integer)));
    }

    public function test_list_of_datetime_with_schema() : void
    {
        $this->assertEquals(
            Entry::list_of_datetime('e', $list = [new \DateTimeImmutable('now'), new \DateTimeImmutable('tomorrow')]),
            (new NativeEntryFactory())
                ->create('e', $list, new Schema(Schema\Definition::list('e', ObjectType::fromString(\DateTimeInterface::class))))
        );
    }

    public function test_list_of_datetimes() : void
    {
        $this->assertEquals(
            Entry::list_of_objects('e', \DateTimeInterface::class, $list = [new \DateTimeImmutable(), new \DateTime()]),
            (new NativeEntryFactory())->create('e', $list)
        );
    }

    public function test_list_of_scalars() : void
    {
        $this->assertEquals(
            Entry::list_of_int('e', [1, 2]),
            (new NativeEntryFactory())->create('e', [1, 2])
        );
    }

    public function test_list_of_string_datetime_with_schema() : void
    {
        $this->assertEquals(
            Entry::list_of_datetime('e', [new \DateTimeImmutable('2022-01-01 00:00:00 UTC'), new \DateTimeImmutable('2022-01-01 00:00:00 UTC')]),
            (new NativeEntryFactory())
                ->create(
                    'e',
                    ['2022-01-01 00:00:00 UTC', '2022-01-01 00:00:00 UTC'],
                    new Schema(Schema\Definition::list('e', ObjectType::fromString(\DateTimeInterface::class)))
                )
        );
    }

    public function test_nested_structure() : void
    {
        $this->assertEquals(
            Entry::structure(
                'address',
                Entry::string('city', 'Krakow'),
                Entry::structure(
                    'geo',
                    Entry::float('lat', 50.06143),
                    Entry::float('lon', 19.93658)
                ),
                Entry::string('street', 'Floriańska'),
                Entry::string('zip', '31-021')
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
            Entry::null('e'),
            (new NativeEntryFactory())->create('e', null)
        );
    }

    public function test_null_with_schema() : void
    {
        $this->assertEquals(
            Entry::null('e'),
            (new NativeEntryFactory())->create('e', null, new Schema(Schema\Definition::null('e')))
        );

        $this->assertEquals(
            Entry::null('e'),
            (new NativeEntryFactory())->create('e', null, new Schema(Schema\Definition::string('e', true)))
        );
    }

    public function test_object() : void
    {
        $this->assertEquals(
            Entry::object('e', $object = new \ArrayIterator([1, 2])),
            (new NativeEntryFactory())->create('e', $object)
        );
    }

    public function test_object_with_schema() : void
    {
        $this->assertEquals(
            Entry::object('e', $object = new \ArrayObject([1, 2, 3])),
            (new NativeEntryFactory())
                ->create('e', $object, new Schema(Schema\Definition::object('e')))
        );
    }

    public function test_string() : void
    {
        $this->assertEquals(
            Entry::string('e', 'test'),
            (new NativeEntryFactory())->create('e', 'test')
        );
    }

    public function test_string_with_schema() : void
    {
        $this->assertEquals(
            Entry::string('e', 'string'),
            (new NativeEntryFactory())->create('e', 'string', new Schema(Schema\Definition::string('e')))
        );
    }

    public function test_structure() : void
    {
        $this->assertEquals(
            Entry::structure(
                'address',
                Entry::string('city', 'Krakow'),
                Entry::string('street', 'Floriańska'),
                Entry::string('zip', '31-021')
            ),
            (new NativeEntryFactory())->create('address', ['city' => 'Krakow', 'street' => 'Floriańska', 'zip' => '31-021'])
        );
    }

    #[DataProvider('provide_unrecognized_data')]
    public function test_unrecognized_data_set_same_as_provided(string $input) : void
    {
        $this->assertEquals(
            Entry::string('e', $input),
            (new NativeEntryFactory())->create('e', $input)
        );
    }

    public function test_uuid_from_ramsey_uuid_library() : void
    {
        if (!\class_exists(\Ramsey\Uuid\Uuid::class)) {
            $this->markTestSkipped("Package 'ramsey/uuid' is required for this test.");
        }

        $this->assertEquals(
            Entry::uuid('e', $uuid = \Ramsey\Uuid\Uuid::uuid4()->toString()),
            (new NativeEntryFactory())->create('e', $uuid)
        );
    }

    public function test_uuid_from_string() : void
    {
        $this->assertEquals(
            Entry::uuid('e', $uuid = '00000000-0000-0000-0000-000000000000'),
            (new NativeEntryFactory())->create('e', $uuid)
        );
    }

    public function test_uuid_string_with_uuid_definition_provided() : void
    {
        $this->assertEquals(
            Entry::uuid('e', $uuid = '00000000-0000-0000-0000-000000000000'),
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
            Entry::xml('e', $xml),
            (new NativeEntryFactory())->create('e', $doc)
        );
    }

    public function test_xml_from_string() : void
    {
        $this->assertEquals(
            Entry::xml('e', $xml = '<root><foo>1</foo><bar>2</bar><baz>3</baz></root>'),
            (new NativeEntryFactory())->create('e', $xml)
        );
    }

    public function test_xml_string_with_xml_definition_provided() : void
    {
        $this->assertEquals(
            Entry::xml('e', $xml = '<root><foo>1</foo><bar>2</bar><baz>3</baz></root>'),
            (new NativeEntryFactory())->create('e', $xml, new Schema(Schema\Definition::xml('e')))
        );
    }
}
