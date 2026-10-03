<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use Flow\ETL\Column\AdaptiveBackend;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Double\SpyBackend;
use Flow\Floe\Exception\FloeException;
use Flow\Floe\FloeSerializer;
use Flow\Floe\Format;
use Flow\Floe\Tests\Mother\RowsMother;
use Flow\Serializer\Exception\SerializationException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function array_map;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
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
            'single row' => [array_to_rows(
                [['id' => 1, 'name' => 'John']],
                schema(int_schema('id'), str_schema('name')),
            )],
            'rows' => [array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id')))],
            'heterogeneous rows' => [RowsMother::heterogeneous()],
            'all entry types' => [RowsMother::withAllEntryTypes()],
            'empty rows' => [rows(schema())],
            'empty rows with a declared column' => [rows(schema(str_schema('country')))],
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
        $serializer = new FloeSerializer(new AdaptiveBackend());

        static::assertEquals($value, unserialize_from_string($serializer, serialize_to_string($serializer, $value)));
    }

    public function test_streaming_mode_round_trip(): void
    {
        $serializer = new FloeSerializer(new AdaptiveBackend(), 2);
        $value = RowsMother::withAllEntryTypes();

        static::assertEquals($value, unserialize_from_string($serializer, serialize_to_string($serializer, $value)));
    }

    public function test_cache_round_trip_preserves_rows_without_a_partition_table(): void
    {
        $value = array_to_rows(
            [['id' => 1, 'country' => 'PL'], ['id' => 2, 'country' => 'PL']],
            schema(int_schema('id'), str_schema('country')),
        );

        static::assertEquals($value, unserialize_from_string(
            new FloeSerializer(new AdaptiveBackend(), 1),
            serialize_to_string(new FloeSerializer(new AdaptiveBackend()), $value),
        ));
    }

    public function test_heterogeneous_rows_round_trip_unpadded_but_union_widened(): void
    {
        $serializer = new FloeSerializer(new AdaptiveBackend());
        // one write session = one schema: rows keep their own columns (unpadded) but
        // entry types widen to the batch union - id and name become nullable because
        // each is absent from some row. Values are unchanged; only nullability widens.
        $value = array_to_rows(
            [['id' => 1], ['id' => 2, 'name' => 'John'], ['name' => 'Jane']],
            schema(int_schema('id', nullable: true), str_schema('name', nullable: true)),
        );

        $result = unserialize_from_string($serializer, serialize_to_string($serializer, $value));

        static::assertSame($value->toArray(), $result->toArray());
        static::assertTrue($result->schema()->findDefinition('id')?->isNullable());
        static::assertTrue($result->schema()->findDefinition('name')?->isNullable());
    }

    #[DataProvider('values')]
    public function test_read_batch_size_does_not_change_decoded_rows(Rows $value): void
    {
        $bytes = serialize_to_string(new FloeSerializer(new AdaptiveBackend()), $value);

        static::assertEquals(
            unserialize_from_string(new FloeSerializer(new AdaptiveBackend()), $bytes),
            unserialize_from_string(new FloeSerializer(new AdaptiveBackend(), 1), $bytes),
        );
    }

    #[DataProvider('exactRoundTripValues')]
    public function test_batch_sizes_decode_each_others_bytes(Rows $value): void
    {
        $small = new FloeSerializer(new AdaptiveBackend(), 1);
        $default = new FloeSerializer(new AdaptiveBackend());

        static::assertEquals($value, unserialize_from_string($default, serialize_to_string($small, $value)));
        static::assertEquals($value, unserialize_from_string($small, serialize_to_string($default, $value)));
    }

    public function test_batch_size_smaller_than_row_count(): void
    {
        $serializer = new FloeSerializer(new AdaptiveBackend(), 3);
        $value = array_to_rows(
            array_map(static fn(int $id): array => ['id' => $id], range(1, 10)),
            schema(int_schema('id')),
        );

        static::assertEquals($value, unserialize_from_string($serializer, serialize_to_string(
            new FloeSerializer(new AdaptiveBackend()),
            $value,
        )));
    }

    public function test_batch_size_larger_than_row_count(): void
    {
        $serializer = new FloeSerializer(new AdaptiveBackend(), 100);
        $value = array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id')));

        static::assertEquals($value, unserialize_from_string($serializer, serialize_to_string($serializer, $value)));
    }

    public function test_batch_size_below_one_throws(): void
    {
        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('Serializer batch size must be at least 1');

        // @mago-ignore analysis:invalid-argument
        new FloeSerializer(new AdaptiveBackend(), 0);
    }

    public function test_unserialize_rejects_torn_bytes(): void
    {
        $this->expectException(SerializationException::class);

        unserialize_from_string(new FloeSerializer(new AdaptiveBackend()), 'not a valid floe payload');
    }

    public function test_unserialize_rejects_empty_payload(): void
    {
        $this->expectException(SerializationException::class);
        $this->expectExceptionMessage('too small');

        unserialize_from_string(new FloeSerializer(new AdaptiveBackend()), '');
    }

    public function test_unserialize_rejects_row_count_mismatch(): void
    {
        $bytes = serialize_to_string(new FloeSerializer(new AdaptiveBackend()), array_to_rows([[
            'id' => 1,
        ]], schema(int_schema('id'))));
        // inflate the footer's row count while the body still holds a single row; the JSON
        // byte-length is unchanged (1 -> 2) so the trailer stays valid and the read reaches
        // the whole-value row-count guard
        $corrupted = str_replace('"statistics":{"rows":1', '"statistics":{"rows":2', $bytes);

        $this->expectException(SerializationException::class);
        $this->expectExceptionMessage('decoded 1 of 2 rows');

        unserialize_from_string(new FloeSerializer(new AdaptiveBackend()), $corrupted);
    }

    public function test_unserialize_rejects_payload_smaller_than_header_and_trailer(): void
    {
        $this->expectException(SerializationException::class);
        $this->expectExceptionMessage('too small');

        unserialize_from_string(new FloeSerializer(new AdaptiveBackend()), 'tiny');
    }

    public function test_unserialize_rejects_footer_that_does_not_fit(): void
    {
        $this->expectException(SerializationException::class);
        $this->expectExceptionMessage('footer does not fit');

        unserialize_from_string(
            new FloeSerializer(new AdaptiveBackend()),
            Format::header(0x00) . Format::trailer(1000),
        );
    }

    public function test_unserialize_builds_through_the_given_backend(): void
    {
        $backend = new SpyBackend();
        $serializer = new FloeSerializer(backend: $backend);

        unserialize_from_string($serializer, serialize_to_string($serializer, RowsMother::numbered(3)));

        static::assertGreaterThanOrEqual(1, $backend->decodes());
    }

    public function test_an_empty_payload_unserializes_to_an_empty_batch_of_its_schema(): void
    {
        $serializer = new FloeSerializer(new AdaptiveBackend());
        $rows = unserialize_from_string($serializer, serialize_to_string($serializer, rows(schema(int_schema('id')))));

        static::assertSame(0, $rows->count());
        static::assertSame(schema(int_schema('id'))->normalize(), $rows->schema()->normalize());
    }
}
