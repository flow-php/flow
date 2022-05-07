<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Factory;

use Flow\ETL\DSL\Entry;
use Flow\ETL\Row\Factory\NativeEntryFactory;
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

    public function test_bool() : void
    {
        $this->assertEquals(
            Entry::boolean('e', false),
            (new NativeEntryFactory())->create('e', false)
        );
    }

    public function test_datetime() : void
    {
        $this->assertEquals(
            Entry::datetime('e', $now = new \DateTimeImmutable()),
            (new NativeEntryFactory())->create('e', $now)
        );
    }

    public function test_float() : void
    {
        $this->assertEquals(
            Entry::float('e', 1.1),
            (new NativeEntryFactory())->create('e', 1.1)
        );
    }

    public function test_int() : void
    {
        $this->assertEquals(
            Entry::integer('e', 1),
            (new NativeEntryFactory())->create('e', 1)
        );
    }

    public function test_json() : void
    {
        $this->assertEquals(
            Entry::json_object('e', ['id' => 1]),
            (new NativeEntryFactory())->create('e', '{"id":1}')
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

    public function test_null() : void
    {
        $this->assertEquals(
            Entry::null('e'),
            (new NativeEntryFactory())->create('e', null)
        );
    }

    public function test_object() : void
    {
        $this->assertEquals(
            Entry::object('e', $object = new \ArrayIterator([1, 2])),
            (new NativeEntryFactory())->create('e', $object)
        );
    }

    public function test_string() : void
    {
        $this->assertEquals(
            Entry::string('e', 'test'),
            (new NativeEntryFactory())->create('e', 'test')
        );
    }
}
