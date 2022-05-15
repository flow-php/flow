<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Factory;

use Flow\ETL\DSL\Entry;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entry\TypedCollection\ObjectType;
use Flow\ETL\Row\Entry\TypedCollection\ScalarType;
use Flow\ETL\Row\Factory\NativeEntryFactory;
use Flow\ETL\Row\Schema;
use Flow\ETL\Tests\Fixtures\Enum\BackedIntEnum;
use PHPUnit\Framework\TestCase;

final class NativeEntryFactoryTest extends TestCase
{
    public function test_array() : void
    {
        $this->assertEquals(
            Entry::array('e', ['a' => 1, 'b' => 2]),
            (new NativeEntryFactory())->create('e', ['a' => 1, 'b' => 2])
        );
    }

    public function test_array_with_schema() : void
    {
        $this->assertEquals(
            Entry::array('e', [1, 2, 3]),
            (new NativeEntryFactory(new Schema(Schema\Definition::array('e'))))
                ->create('e', [1, 2, 3])
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
            (new NativeEntryFactory(new Schema(Schema\Definition::boolean('e'))))->create('e', false)
        );
    }

    public function test_conversion_to_different_type_with_schema() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Can't convert value into entry \"e\"");

        (new NativeEntryFactory(new Schema(Schema\Definition::string('e'))))
            ->create('e', 1);
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
            (new NativeEntryFactory(new Schema(Schema\Definition::dateTime('e'))))
                ->create('e', '2022-01-01 00:00:00 UTC')
        );
    }

    public function test_datetime_with_schema() : void
    {
        $this->assertEquals(
            Entry::datetime('e', $datetime = new \DateTimeImmutable('now')),
            (new NativeEntryFactory(new Schema(Schema\Definition::dateTime('e'))))
                ->create('e', $datetime)
        );
    }

    public function test_enum_invalid_value_with_schema() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Value \"not_valid\" can't be converted to " . BackedIntEnum::class . ' enum');

        (new NativeEntryFactory(new Schema(Schema\Definition::enum('e', BackedIntEnum::class))))
            ->create('e', 'not_valid');
    }

    public function test_enum_with_schema() : void
    {
        $this->assertEquals(
            Entry::enum('e', BackedIntEnum::one),
            (new NativeEntryFactory(new Schema(Schema\Definition::enum('e', BackedIntEnum::class))))
                ->create('e', 'one')
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
            (new NativeEntryFactory(new Schema(Schema\Definition::float('e'))))->create('e', 1.1)
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
            (new NativeEntryFactory(new Schema(Schema\Definition::integer('e'))))->create('e', 1)
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
            (new NativeEntryFactory(new Schema(Schema\Definition::json('e'))))->create('e', [['id' => 1]])
        );
    }

    public function test_json_object_array_with_schema() : void
    {
        $this->assertEquals(
            Entry::json_object('e', ['id' => 1]),
            (new NativeEntryFactory(new Schema(Schema\Definition::json('e'))))->create('e', ['id' => 1])
        );
    }

    public function test_json_string_with_schema() : void
    {
        $this->assertEquals(
            Entry::json_string('e', '{"id": 1}'),
            (new NativeEntryFactory(new Schema(Schema\Definition::json('e'))))->create('e', '{"id": 1}')
        );
    }

    public function test_list_int_with_schema() : void
    {
        $this->assertEquals(
            Entry::list_of_int('e', [1, 2, 3]),
            (new NativeEntryFactory(new Schema(Schema\Definition::list('e', ScalarType::integer))))->create('e', [1, 2, 3])
        );
    }

    public function test_list_int_with_schema_but_string_list() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Field "e" conversion exception. Expected list of integer got different types.');

        (new NativeEntryFactory(new Schema(Schema\Definition::list('e', ScalarType::integer))))->create('e', ['1', '2', '3']);
    }

    public function test_list_of_datetime_with_schema() : void
    {
        $this->assertEquals(
            Entry::list_of_datetime('e', $list = [new \DateTimeImmutable('now'), new \DateTimeImmutable('tomorrow')]),
            (new NativeEntryFactory(new Schema(Schema\Definition::list('e', ObjectType::of(\DateTimeInterface::class)))))
                ->create('e', $list)
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
            (new NativeEntryFactory(new Schema(Schema\Definition::list('e', ObjectType::of(\DateTimeInterface::class)))))
                ->create('e', ['2022-01-01 00:00:00 UTC', '2022-01-01 00:00:00 UTC'])
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
            (new NativeEntryFactory(new Schema(Schema\Definition::null('e'))))->create('e', null)
        );

        $this->assertEquals(
            Entry::null('e'),
            (new NativeEntryFactory(new Schema(Schema\Definition::string('e', true))))->create('e', null)
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
            (new NativeEntryFactory(new Schema(Schema\Definition::object('e'))))
                ->create('e', $object)
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
            (new NativeEntryFactory(new Schema(Schema\Definition::string('e'))))->create('e', 'string')
        );
    }
}
