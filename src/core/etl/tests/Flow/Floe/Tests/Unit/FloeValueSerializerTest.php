<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\Filesystem\Partition;
use Flow\Floe\Exception\FloeException;
use Flow\Floe\FloeValueSerializer;
use Flow\Floe\Format;
use Flow\Floe\Tests\Mother\RowsMother;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function array_map;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\str_entry;
use function range;

final class FloeValueSerializerTest extends TestCase
{
    /**
     * @return array<string, array{Row|Rows}>
     */
    public static function values(): array
    {
        return [
            'row' => [row(int_entry('id', 1), str_entry('name', 'John'))],
            'rows' => [rows(row(int_entry('id', 1)), row(int_entry('id', 2)))],
            'partitioned rows' => [RowsMother::partitioned()],
            'heterogeneous rows' => [RowsMother::heterogeneous()],
            'all entry types' => [RowsMother::withAllEntryTypes()],
            'empty rows' => [rows()],
            'empty partitioned rows' => [Rows::partitioned([], [new Partition('country', 'PL')])],
        ];
    }

    #[DataProvider('values')]
    public function test_batch_size_does_not_change_bytes(Row|Rows $value): void
    {
        static::assertSame((new FloeValueSerializer(1))->encode($value), (new FloeValueSerializer())->encode($value));
    }

    #[DataProvider('values')]
    public function test_batch_sizes_decode_each_others_bytes(Row|Rows $value): void
    {
        $small = new FloeValueSerializer(1);
        $default = new FloeValueSerializer();

        static::assertEquals($value, $default->decode($small->encode($value)));
        static::assertEquals($value, $small->decode($default->encode($value)));
    }

    public function test_decode_rejects_torn_bytes(): void
    {
        $this->expectException(FloeException::class);

        (new FloeValueSerializer())->decode('not a valid floe payload');
    }

    public function test_decode_rejects_payload_smaller_than_header_and_trailer(): void
    {
        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('too small');

        (new FloeValueSerializer())->decode('tiny');
    }

    public function test_decode_rejects_footer_that_does_not_fit(): void
    {
        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('footer does not fit');

        (new FloeValueSerializer())->decode(Format::header(0x00) . Format::trailer(1000));
    }

    public function test_batch_size_smaller_than_row_count(): void
    {
        $codec = new FloeValueSerializer(3);
        $value = rows(...array_map(static fn(int $id): Row => row(int_entry('id', $id)), range(1, 10)));

        static::assertSame((new FloeValueSerializer())->encode($value), $codec->encode($value));
        static::assertEquals($value, $codec->decode($codec->encode($value)));
    }

    public function test_batch_size_larger_than_row_count(): void
    {
        $codec = new FloeValueSerializer(100);
        $value = rows(row(int_entry('id', 1)), row(int_entry('id', 2)));

        static::assertEquals($value, $codec->decode($codec->encode($value)));
    }

    #[DataProvider('values')]
    public function test_round_trip(Row|Rows $value): void
    {
        $codec = new FloeValueSerializer();

        static::assertEquals($value, $codec->decode($codec->encode($value)));
    }

    public function test_heterogeneous_rows_round_trip_exactly_unpadded(): void
    {
        $codec = new FloeValueSerializer();
        // rows with different schemas must come back as written - NOT padded to a merged schema
        $value = rows(
            row(int_entry('id', 1)),
            row(int_entry('id', 2), str_entry('name', 'John')),
            row(str_entry('name', 'Jane')),
        );

        static::assertEquals($value, $codec->decode($codec->encode($value)));
    }

    public function test_round_trip_all_entry_types(): void
    {
        $codec = new FloeValueSerializer();
        $value = RowsMother::withAllEntryTypes();

        static::assertEquals($value, $codec->decode($codec->encode($value)));
    }

    public function test_round_trip_empty_rows(): void
    {
        $codec = new FloeValueSerializer();

        static::assertEquals(rows(), $codec->decode($codec->encode(rows())));
    }

    public function test_round_trip_partitioned_rows(): void
    {
        $codec = new FloeValueSerializer();
        $value = RowsMother::partitioned();

        static::assertEquals($value, $codec->decode($codec->encode($value)));
    }

    public function test_round_trip_row(): void
    {
        $codec = new FloeValueSerializer();
        $value = row(int_entry('id', 1), str_entry('name', 'John'));

        $result = $codec->decode($codec->encode($value));

        static::assertInstanceOf(Row::class, $result);
        static::assertEquals($value, $result);
    }

    public function test_round_trip_rows(): void
    {
        $codec = new FloeValueSerializer();
        $value = rows(row(int_entry('id', 1)), row(int_entry('id', 2)));

        $result = $codec->decode($codec->encode($value));

        static::assertInstanceOf(Rows::class, $result);
        static::assertEquals($value, $result);
    }
}
