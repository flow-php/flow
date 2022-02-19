<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Row;
use Flow\ETL\Row\Entry\ArrayEntry;
use Flow\ETL\Row\Entry\DateTimeEntry;
use Flow\ETL\Row\Entry\IntegerEntry;
use Flow\ETL\Row\Entry\JsonEntry;
use Flow\ETL\Row\Entry\NullEntry;
use Flow\ETL\Row\Entry\StringEntry;
use Flow\ETL\Rows;
use Flow\ETL\Transformer\Cast\CastJsonToArray;
use Flow\ETL\Transformer\Cast\CastToArray;
use Flow\ETL\Transformer\Cast\CastToDateTime;
use Flow\ETL\Transformer\Cast\CastToInteger;
use Flow\ETL\Transformer\Cast\CastToJson;
use Flow\ETL\Transformer\Cast\CastToString;
use Flow\ETL\Transformer\CastTransformer;
use PHPUnit\Framework\TestCase;

final class CastTransformerTest extends TestCase
{
    public function test_datetime_string_to_datetime_transformer() : void
    {
        $entry = new StringEntry('date', '2020-01-01 00:00:00 UTC');

        $transformer = new CastTransformer(new CastToDateTime(['date']));

        $rows = $transformer->transform(new Rows(new Row(new Row\Entries($entry))));

        $this->assertInstanceOf(DateTimeEntry::class, $rows->first()->get('date'));
        $this->assertEquals(new \DateTimeImmutable('2020-01-01 00:00:00.+00:00'), $rows->first()->valueOf('date'));
    }

    public function test_datetime_nullable_string_to_datetime_transformer() : void
    {
        $entry = new NullEntry('date');

        $transformer = new CastTransformer(CastToDateTime::nullable(['date'], 'Y-m-d H:i:s.P'));

        $rows = $transformer->transform(new Rows(new Row(new Row\Entries($entry))));

        $this->assertInstanceOf(NullEntry::class, $rows->first()->get('date'));
        $this->assertNull($rows->first()->valueOf('date'));
    }

    public function test_multiple_datetime_strings_to_datetime_transformer() : void
    {
        $start = new StringEntry('start_date', '2020-01-01 00:00:00 UTC');
        $current = new StringEntry('current_date', '2020-01-01 01:00:00 UTC');
        $end = new StringEntry('end_date', '2020-01-01 02:00:00 UTC');

        $transformer = new CastTransformer(new CastToDateTime(['start_date', 'end_date']));

        $rows = $transformer->transform(new Rows(new Row(new Row\Entries($start, $current, $end))));

        $this->assertInstanceOf(DateTimeEntry::class, $rows->first()->get('start_date'));
        $this->assertEquals(new \DateTimeImmutable('2020-01-01 00:00:00.+00:00'), $rows->first()->valueOf('start_date'));

        $this->assertInstanceOf(StringEntry::class, $rows->first()->get('current_date'));
        $this->assertEquals('2020-01-01 01:00:00 UTC', $rows->first()->valueOf('current_date'));

        $this->assertInstanceOf(DateTimeEntry::class, $rows->first()->get('end_date'));
        $this->assertEquals(new \DateTimeImmutable('2020-01-01 02:00:00.+00:00'), $rows->first()->valueOf('end_date'));
    }

    public function test_datetime_string_without_tz() : void
    {
        $entry = new StringEntry('date', '2020-01-01 00:00:00');

        $transformer = new CastTransformer(new CastToDateTime(['date'], 'America/Los_Angeles'));

        $rows = $transformer->transform(new Rows(new Row(new Row\Entries($entry))));

        $this->assertInstanceOf(DateTimeEntry::class, $rows->first()->get('date'));
        $this->assertEquals(new \DateTimeImmutable('2020-01-01 00:00:00.-08:00'), $rows->first()->valueOf('date'));
    }

    public function test_datetime_string_without_tz_to_utc() : void
    {
        $entry = new StringEntry('date', '2020-01-01 00:00:00.-08:00');

        $transformer = new CastTransformer(new CastToDateTime(['date'], null, 'UTC'));

        $rows = $transformer->transform(new Rows(new Row(new Row\Entries($entry))));

        $this->assertInstanceOf(DateTimeEntry::class, $rows->first()->get('date'));
        $this->assertEquals(new \DateTimeImmutable('2020-01-01 08:00:00.+00:00'), $rows->first()->valueOf('date'));
    }

    public function test_datetime_string_without_tz_to_tz() : void
    {
        $entry = new StringEntry('date', '2020-01-01 00:00:00');

        $transformer = new CastTransformer(new CastToDateTime(['date'], 'UTC', 'America/Los_Angeles'));

        $rows = $transformer->transform(new Rows(new Row(new Row\Entries($entry))));

        $this->assertInstanceOf(DateTimeEntry::class, $rows->first()->get('date'));
        $this->assertEquals(new \DateTimeImmutable('2019-12-31 16:00:00.-08:00'), $rows->first()->valueOf('date'));
    }

    public function test_datetime_string_with_tz_to_tz() : void
    {
        $entry = new StringEntry('date', '2020-01-01 00:00:00 UTC');

        $transformer = new CastTransformer(new CastToDateTime(['date'], null, 'Europe/Warsaw'));

        $rows = $transformer->transform(new Rows(new Row(new Row\Entries($entry))));

        $this->assertInstanceOf(DateTimeEntry::class, $rows->first()->get('date'));
        $this->assertEquals(new \DateTimeImmutable('2020-01-01 01:00:00.+01:00'), $rows->first()->valueOf('date'));
    }

    public function test_string_to_integer() : void
    {
        $entry = new StringEntry('id', '123456');

        $transformer = new CastTransformer(new CastToInteger(['id']));

        $rows = $transformer->transform(new Rows(new Row(new Row\Entries($entry))));

        $this->assertInstanceOf(Row\Entry\IntegerEntry::class, $rows->first()->get('id'));
        $this->assertSame(123456, $rows->first()->valueOf('id'));
    }

    public function test_cast_array_to_json() : void
    {
        $entry = new ArrayEntry('collection', ['foo' => 'bar']);

        $transformer = new CastTransformer(new CastToJson(['collection']));

        $rows = $transformer->transform(new Rows(new Row(new Row\Entries($entry))));

        $this->assertInstanceOf(Row\Entry\JsonEntry::class, $rows->first()->get('collection'));
        $this->assertSame('{"foo":"bar"}', $rows->first()->valueOf('collection'));
    }

    public function test_integer_to_string() : void
    {
        $entry = new IntegerEntry('id', 123456);

        $transformer = new CastTransformer(new CastToString(['id']));

        $rows = $transformer->transform(new Rows(new Row(new Row\Entries($entry))));

        $this->assertInstanceOf(Row\Entry\StringEntry::class, $rows->first()->get('id'));
        $this->assertSame('123456', $rows->first()->valueOf('id'));
    }

    public function test_integer_to_array() : void
    {
        $entry = new IntegerEntry('ids', 123456);

        $transformer = new CastTransformer(new CastToArray(['ids']));

        $rows = $transformer->transform(new Rows(new Row(new Row\Entries($entry))));

        $this->assertInstanceOf(Row\Entry\ArrayEntry::class, $rows->first()->get('ids'));
        $this->assertSame([123456], $rows->first()->valueOf('ids'));
    }

    public function test_json_to_array() : void
    {
        $entry = new JsonEntry('ids', [123456]);

        $transformer = new CastTransformer(new CastJsonToArray(['ids']));

        $rows = $transformer->transform(new Rows(new Row(new Row\Entries($entry))));

        $this->assertInstanceOf(Row\Entry\ArrayEntry::class, $rows->first()->get('ids'));
        $this->assertSame([123456], $rows->first()->valueOf('ids'));
    }

    public function test_string_json_to_array() : void
    {
        $entry = new StringEntry('ids', '[123456]');

        $transformer = new CastTransformer(new CastJsonToArray(['ids']));

        $rows = $transformer->transform(new Rows(new Row(new Row\Entries($entry))));

        $this->assertInstanceOf(Row\Entry\ArrayEntry::class, $rows->first()->get('ids'));
        $this->assertSame([123456], $rows->first()->valueOf('ids'));
    }

    public function test_casts_multiple_entries_with_null_entry_in_betwee() : void
    {
        $transformer = new CastTransformer(new CastToInteger(['id', 'limit', 'current'], $nullable = true));

        $rows = $transformer->transform(new Rows(
            Row::create(
                new StringEntry('id', '1'),
                new NullEntry('limit'),
                new StringEntry('current', '10')
            )
        ));

        $this->assertInstanceOf(IntegerEntry::class, $rows->first()->get('id'));
        $this->assertSame(1, $rows->first()->valueOf('id'));

        $this->assertInstanceOf(NullEntry::class, $rows->first()->get('limit'));

        $this->assertInstanceOf(IntegerEntry::class, $rows->first()->get('current'));
        $this->assertSame(10, $rows->first()->valueOf('current'));
    }
}
