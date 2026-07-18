<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\Filesystem\Partition;
use Flow\Floe\Exception\FloeException;
use Flow\Floe\FloeSerializer;
use Flow\Floe\Format;
use Flow\Floe\Tests\Mother\RowsMother;
use Flow\Serializer\Exception\SerializationException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function array_map;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\str_entry;
use function Flow\Serializer\DSL\serialize_to_string;
use function Flow\Serializer\DSL\unserialize_from_string;
use function range;
use function str_replace;

final class FloeSerializerTest extends TestCase
{
    /**
     * @return array<string, array{Rows}>
     */
    public static function values(): array
    {
        return [
            'single row' => [rows(row(int_entry('id', 1), str_entry('name', 'John')))],
            'rows' => [rows(row(int_entry('id', 1)), row(int_entry('id', 2)))],
            'partitioned rows' => [RowsMother::partitioned()],
            'heterogeneous rows' => [RowsMother::heterogeneous()],
            'all entry types' => [RowsMother::withAllEntryTypes()],
            'empty rows' => [rows()],
            'empty partitioned rows' => [Rows::partitioned([], [new Partition('country', 'PL')])],
        ];
    }

    /**
     * Heterogeneous rows no longer round-trip byte-for-type-exact: one write
     * session carries one schema, so absent-in-some-row columns widen to nullable.
     * That residue is covered by test_heterogeneous_rows_round_trip_unpadded_but_union_widened.
     *
     * @return array<string, array{Rows}>
     */
    public static function exactRoundTripValues(): array
    {
        $values = self::values();
        unset($values['heterogeneous rows']);

        return $values;
    }

    #[DataProvider('exactRoundTripValues')]
    public function test_serialize_unserialize_round_trip(Rows $value): void
    {
        $serializer = new FloeSerializer();

        static::assertEquals($value, unserialize_from_string($serializer, serialize_to_string($serializer, $value)));
    }

    public function test_streaming_mode_round_trip(): void
    {
        $serializer = new FloeSerializer(2);
        $value = RowsMother::withAllEntryTypes();

        static::assertEquals($value, unserialize_from_string($serializer, serialize_to_string($serializer, $value)));
    }

    public function test_streaming_serialization_is_byte_identical_to_bulk(): void
    {
        $value = RowsMother::heterogeneous();

        static::assertSame(
            serialize_to_string(new FloeSerializer(1), $value),
            serialize_to_string(new FloeSerializer(), $value),
        );
    }

    public function test_heterogeneous_rows_round_trip_unpadded_but_union_widened(): void
    {
        $serializer = new FloeSerializer();
        // one write session = one schema: rows keep their own columns (unpadded) but
        // entry types widen to the batch union - id and name become nullable because
        // each is absent from some row. Values are unchanged; only nullability widens.
        $value = rows(
            row(int_entry('id', 1)),
            row(int_entry('id', 2), str_entry('name', 'John')),
            row(str_entry('name', 'Jane')),
        );

        $result = unserialize_from_string($serializer, serialize_to_string($serializer, $value));

        static::assertSame($value->toArray(), $result->toArray());
        static::assertTrue($result->schema()->findDefinition('id')?->isNullable());
        static::assertTrue($result->schema()->findDefinition('name')?->isNullable());
    }

    #[DataProvider('values')]
    public function test_batch_size_does_not_change_bytes(Rows $value): void
    {
        static::assertSame(
            serialize_to_string(new FloeSerializer(1), $value),
            serialize_to_string(new FloeSerializer(), $value),
        );
    }

    #[DataProvider('exactRoundTripValues')]
    public function test_batch_sizes_decode_each_others_bytes(Rows $value): void
    {
        $small = new FloeSerializer(1);
        $default = new FloeSerializer();

        static::assertEquals($value, unserialize_from_string($default, serialize_to_string($small, $value)));
        static::assertEquals($value, unserialize_from_string($small, serialize_to_string($default, $value)));
    }

    public function test_batch_size_smaller_than_row_count(): void
    {
        $serializer = new FloeSerializer(3);
        $value = rows(...array_map(static fn(int $id): Row => row(int_entry('id', $id)), range(1, 10)));

        static::assertSame(serialize_to_string(new FloeSerializer(), $value), serialize_to_string($serializer, $value));
        static::assertEquals($value, unserialize_from_string($serializer, serialize_to_string($serializer, $value)));
    }

    public function test_batch_size_larger_than_row_count(): void
    {
        $serializer = new FloeSerializer(100);
        $value = rows(row(int_entry('id', 1)), row(int_entry('id', 2)));

        static::assertEquals($value, unserialize_from_string($serializer, serialize_to_string($serializer, $value)));
    }

    public function test_batch_size_below_one_throws(): void
    {
        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('Serializer batch size must be at least 1');

        // @mago-ignore analysis:invalid-argument
        new FloeSerializer(0);
    }

    public function test_unserialize_rejects_torn_bytes(): void
    {
        $this->expectException(SerializationException::class);

        unserialize_from_string(new FloeSerializer(), 'not a valid floe payload');
    }

    public function test_unserialize_rejects_empty_payload(): void
    {
        $this->expectException(SerializationException::class);
        $this->expectExceptionMessage('too small');

        unserialize_from_string(new FloeSerializer(), '');
    }

    public function test_unserialize_rejects_row_count_mismatch(): void
    {
        $bytes = serialize_to_string(new FloeSerializer(), rows(row(int_entry('id', 1))));
        // inflate the footer's totalRows while the body still holds a single row; the JSON
        // byte-length is unchanged (1 -> 2) so the trailer stays valid and the read reaches
        // the whole-value row-count guard
        $corrupted = str_replace('"totalRows":1', '"totalRows":2', $bytes);

        $this->expectException(SerializationException::class);
        $this->expectExceptionMessage('decoded 1 of 2 rows');

        unserialize_from_string(new FloeSerializer(), $corrupted);
    }

    public function test_unserialize_rejects_payload_smaller_than_header_and_trailer(): void
    {
        $this->expectException(SerializationException::class);
        $this->expectExceptionMessage('too small');

        unserialize_from_string(new FloeSerializer(), 'tiny');
    }

    public function test_unserialize_rejects_footer_that_does_not_fit(): void
    {
        $this->expectException(SerializationException::class);
        $this->expectExceptionMessage('footer does not fit');

        unserialize_from_string(new FloeSerializer(), Format::header(0x00) . Format::trailer(1000));
    }
}
