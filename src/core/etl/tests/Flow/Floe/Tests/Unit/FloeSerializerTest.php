<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\Floe\FloeSerializer;
use Flow\Floe\Tests\Mother\RowsMother;
use Flow\Serializer\Exception\SerializationException;
use PHPUnit\Framework\TestCase;
use stdClass;

use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\str_entry;

final class FloeSerializerTest extends TestCase
{
    public function test_streaming_mode_round_trip(): void
    {
        $serializer = new FloeSerializer(2);
        $value = RowsMother::withAllEntryTypes();

        static::assertEquals($value, $serializer->unserialize(
            $serializer->serialize($value),
            [Row::class, Rows::class],
        ));
    }

    public function test_streaming_serialization_is_byte_identical_to_bulk(): void
    {
        $value = RowsMother::heterogeneous();

        static::assertSame((new FloeSerializer(1))->serialize($value), (new FloeSerializer())->serialize($value));
    }

    public function test_round_trip_all_entry_types(): void
    {
        $serializer = new FloeSerializer();
        $value = RowsMother::withAllEntryTypes();

        static::assertEquals($value, $serializer->unserialize(
            $serializer->serialize($value),
            [Row::class, Rows::class],
        ));
    }

    public function test_round_trip_empty_rows(): void
    {
        $serializer = new FloeSerializer();

        static::assertEquals(rows(), $serializer->unserialize(
            $serializer->serialize(rows()),
            [Row::class, Rows::class],
        ));
    }

    public function test_round_trip_partitioned_rows(): void
    {
        $serializer = new FloeSerializer();
        $value = RowsMother::partitioned();

        static::assertEquals($value, $serializer->unserialize(
            $serializer->serialize($value),
            [Row::class, Rows::class],
        ));
    }

    public function test_round_trip_row(): void
    {
        $serializer = new FloeSerializer();
        $value = row(int_entry('id', 1), str_entry('name', 'John'));

        $result = $serializer->unserialize($serializer->serialize($value), [Row::class, Rows::class]);

        static::assertInstanceOf(Row::class, $result);
        static::assertEquals($value, $result);
    }

    public function test_round_trip_rows(): void
    {
        $serializer = new FloeSerializer();
        $value = rows(row(int_entry('id', 1)), row(int_entry('id', 2)));

        static::assertEquals($value, $serializer->unserialize(
            $serializer->serialize($value),
            [Row::class, Rows::class],
        ));
    }

    public function test_serialize_rejects_other_objects(): void
    {
        $this->expectException(SerializationException::class);
        $this->expectExceptionMessage('FloeSerializer supports only Rows and Row');

        (new FloeSerializer())->serialize(new stdClass());
    }

    public function test_unserialize_rejects_torn_bytes(): void
    {
        $this->expectException(SerializationException::class);

        (new FloeSerializer())->unserialize('not a floe file', [Row::class, Rows::class]);
    }

    public function test_unserialize_rejects_unexpected_class(): void
    {
        $serializer = new FloeSerializer();

        $this->expectException(SerializationException::class);
        $this->expectExceptionMessage('must return instance of');

        $serializer->unserialize($serializer->serialize(rows(row(int_entry('id', 1)))), [Row::class]);
    }
}
