<?php declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\ParquetFile\RowGroupBuilder;

use Flow\Parquet\ParquetFile\RowGroupBuilder\ColumnChunkStatistics;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use PHPUnit\Framework\TestCase;

final class ColumnChunkStatisticsTest extends TestCase
{
    public function test_statistics_for_boolean() : void
    {
        $statistics = new ColumnChunkStatistics(FlatColumn::boolean('boolean'));

        $statistics->add(true);
        $statistics->add(false);
        $statistics->add(true);
        $statistics->add(true);
        $statistics->add(false);
        $statistics->add(false);
        $statistics->add(null);

        $this->assertFalse($statistics->min());
        $this->assertTrue($statistics->max());
        $this->assertSame(7, $statistics->valuesCount());
        $this->assertSame(2, $statistics->distinctCount());
        $this->assertSame(1, $statistics->nullCount());
    }

    public function test_statistics_for_date() : void
    {
        $statistics = new ColumnChunkStatistics(FlatColumn::date('date'));

        $statistics->add(new \DateTimeImmutable('2020-01-01'));
        $statistics->add(new \DateTimeImmutable('2020-01-02'));
        $statistics->add(new \DateTimeImmutable('2020-01-03'));
        $statistics->add(new \DateTimeImmutable('2020-01-04'));
        $statistics->add(new \DateTimeImmutable('2020-01-05'));
        $statistics->add(new \DateTimeImmutable('2020-01-05'));
        $statistics->add(null);

        $this->assertSame('2020-01-01', $statistics->min()->format('Y-m-d'));
        $this->assertSame('2020-01-05', $statistics->max()->format('Y-m-d'));
        $this->assertSame(7, $statistics->valuesCount());
        $this->assertSame(5, $statistics->distinctCount());
        $this->assertSame(1, $statistics->nullCount());
    }

    public function test_statistics_for_decimal() : void
    {
        $statistics = new ColumnChunkStatistics(FlatColumn::decimal('decimal'));

        $statistics->add('1.1');
        $statistics->add('2.2');
        $statistics->add('3.3');
        $statistics->add('4.4');
        $statistics->add('5.5');
        $statistics->add('5.5');
        $statistics->add(null);

        $this->assertSame('1.1', $statistics->min());
        $this->assertSame('5.5', $statistics->max());
        $this->assertSame(7, $statistics->valuesCount());
        $this->assertSame(5, $statistics->distinctCount());
        $this->assertSame(1, $statistics->nullCount());
    }

    public function test_statistics_for_double() : void
    {
        $statistics = new ColumnChunkStatistics(FlatColumn::double('double'));

        $statistics->add(1.1);
        $statistics->add(2.2);
        $statistics->add(3.3);
        $statistics->add(4.4);
        $statistics->add(5.5);
        $statistics->add(5.5);
        $statistics->add(null);

        $this->assertSame(1.1, $statistics->min());
        $this->assertSame(5.5, $statistics->max());
        $this->assertSame(7, $statistics->valuesCount());
        $this->assertSame(5, $statistics->distinctCount());
        $this->assertSame(1, $statistics->nullCount());
    }

    public function test_statistics_for_enum() : void
    {
        $statistics = new ColumnChunkStatistics(FlatColumn::enum('enum'));

        $statistics->add('a');
        $statistics->add('b');
        $statistics->add('c');
        $statistics->add('d');
        $statistics->add('e');
        $statistics->add('e');
        $statistics->add(null);

        $this->assertSame('a', $statistics->min());
        $this->assertSame('e', $statistics->max());
        $this->assertSame(7, $statistics->valuesCount());
        $this->assertSame(5, $statistics->distinctCount());
        $this->assertSame(1, $statistics->nullCount());
    }

    public function test_statistics_for_float() : void
    {
        $statistics = new ColumnChunkStatistics(FlatColumn::float('float'));

        $statistics->add(1.1);
        $statistics->add(2.2);
        $statistics->add(3.3);
        $statistics->add(4.4);
        $statistics->add(5.5);
        $statistics->add(5.5);
        $statistics->add(null);

        $this->assertSame(1.1, $statistics->min());
        $this->assertSame(5.5, $statistics->max());
        $this->assertSame(7, $statistics->valuesCount());
        $this->assertSame(5, $statistics->distinctCount());
        $this->assertSame(1, $statistics->nullCount());
    }

    public function test_statistics_for_int32() : void
    {
        $statistics = new ColumnChunkStatistics(FlatColumn::int32('int32'));

        $statistics->add(1);
        $statistics->add(2);
        $statistics->add(3);
        $statistics->add(4);
        $statistics->add(5);
        $statistics->add(5);
        $statistics->add(null);

        $this->assertSame(1, $statistics->min());
        $this->assertSame(5, $statistics->max());
        $this->assertSame(7, $statistics->valuesCount());
        $this->assertSame(5, $statistics->distinctCount());
        $this->assertSame(1, $statistics->nullCount());
    }

    public function test_statistics_for_int64() : void
    {
        $statistics = new ColumnChunkStatistics(FlatColumn::int64('int64'));

        $statistics->add(1);
        $statistics->add(2);
        $statistics->add(3);
        $statistics->add(4);
        $statistics->add(5);
        $statistics->add(5);
        $statistics->add(null);

        $this->assertSame(1, $statistics->min());
        $this->assertSame(5, $statistics->max());
        $this->assertSame(7, $statistics->valuesCount());
        $this->assertSame(5, $statistics->distinctCount());
        $this->assertSame(1, $statistics->nullCount());
    }

    public function test_statistics_for_json() : void
    {
        $statistics = new ColumnChunkStatistics(FlatColumn::json('json'));

        $statistics->add('{"a":1}');
        $statistics->add('{"b":2}');
        $statistics->add('{"c":3}');
        $statistics->add('{"d":4}');
        $statistics->add('{"e":5}');
        $statistics->add('{"e":5}');
        $statistics->add(null);

        $this->assertSame('{"a":1}', $statistics->min());
        $this->assertSame('{"e":5}', $statistics->max());
        $this->assertSame(7, $statistics->valuesCount());
        $this->assertSame(5, $statistics->distinctCount());
        $this->assertSame(1, $statistics->nullCount());
    }

    public function test_statistics_for_string() : void
    {
        $statistics = new ColumnChunkStatistics(FlatColumn::string('string'));

        $statistics->add('a');
        $statistics->add('b');
        $statistics->add('c');
        $statistics->add('d');
        $statistics->add('e');
        $statistics->add('e');
        $statistics->add(null);

        $this->assertSame('a', $statistics->min());
        $this->assertSame('e', $statistics->max());
        $this->assertSame(7, $statistics->valuesCount());
        $this->assertSame(5, $statistics->distinctCount());
        $this->assertSame(1, $statistics->nullCount());
    }

    public function test_statistics_for_time() : void
    {
        $statistics = new ColumnChunkStatistics(FlatColumn::time('time'));

        $statistics->add(new \DateInterval('PT1S'));
        $statistics->add(new \DateInterval('PT2S'));
        $statistics->add(new \DateInterval('PT3S'));
        $statistics->add(new \DateInterval('PT4S'));
        $statistics->add(new \DateInterval('PT5S'));
        $statistics->add(new \DateInterval('PT5S'));
        $statistics->add(null);

        $this->assertSame('PT01S', $statistics->min()->format('PT%SS'));
        $this->assertSame('PT05S', $statistics->max()->format('PT%SS'));
        $this->assertSame(7, $statistics->valuesCount());
        $this->assertSame(5, $statistics->distinctCount());
        $this->assertSame(1, $statistics->nullCount());
    }

    public function test_statistics_for_uuid() : void
    {
        $statistics = new ColumnChunkStatistics(FlatColumn::uuid('uuid'));

        $statistics->add('00000000-0000-0000-0000-000000000000');
        $statistics->add('00000000-0000-0000-0000-000000000001');
        $statistics->add('00000000-0000-0000-0000-000000000002');
        $statistics->add('00000000-0000-0000-0000-000000000003');
        $statistics->add('00000000-0000-0000-0000-000000000004');
        $statistics->add('00000000-0000-0000-0000-000000000004');
        $statistics->add(null);

        $this->assertSame('00000000-0000-0000-0000-000000000000', $statistics->min());
        $this->assertSame('00000000-0000-0000-0000-000000000004', $statistics->max());
        $this->assertSame(7, $statistics->valuesCount());
        $this->assertSame(5, $statistics->distinctCount());
        $this->assertSame(1, $statistics->nullCount());
    }
}
