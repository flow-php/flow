<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet\Tests\Unit\RowsNormalizer;

use function Flow\ETL\DSL\{date_entry,
    enum_entry,
    json_entry,
    list_entry,
    time_entry,
    type_list,
    type_string,
    uuid_entry};
use Flow\ETL\Adapter\GoogleSheet\RowsNormalizer\EntryNormalizer;
use Flow\ETL\Adapter\GoogleSheet\ValueInputOption;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Entry\{DateTimeEntry};
use PHPUnit\Framework\TestCase;

final class EntryNormalizerTest extends TestCase
{
    private EntryNormalizer $normalizer;

    protected function setUp() : void
    {
        $this->normalizer = new EntryNormalizer(
            'Y-m-d H:i:s',
            'Y-m-d',
        );
    }

    public function test_normalize_date_entry() : void
    {
        $date = new \DateTimeImmutable('2024-03-15');
        $entry = date_entry('entry', $date);

        $result = $this->normalizer->normalize($entry);

        self::assertEquals('2024-03-15', $result);
    }

    public function test_normalize_datetime_entry() : void
    {
        $dateTime = new \DateTimeImmutable('2024-03-15 14:30:00');
        $entry = new DateTimeEntry('entry', $dateTime);

        $result = $this->normalizer->normalize($entry);

        self::assertEquals($dateTime->format('Y-m-d H:i:s'), $result);
    }

    public function test_normalize_default_entry() : void
    {
        $value = 'simple string';
        $entry = $this->createMock(Entry::class);
        $entry->method('value')->willReturn($value);

        $result = $this->normalizer->normalize($entry);

        self::assertEquals($value, $result);
    }

    public function test_normalize_enum_entry() : void
    {
        $enum = ValueInputOption::RAW;
        $entry = enum_entry('entry', $enum);

        $result = $this->normalizer->normalize($entry);

        self::assertEquals('RAW', $result);
    }

    public function test_normalize_json_entry() : void
    {
        $data = ['key' => 'value'];
        $entry = json_entry('entry', $data);

        $result = $this->normalizer->normalize($entry);

        self::assertEquals(json_encode($data), $result);
    }

    public function test_normalize_list_entry() : void
    {
        $list = ['item1', 'item2'];
        $entry = list_entry('entry', $list, type_list(type_string()));

        $result = $this->normalizer->normalize($entry);

        self::assertEquals(json_encode($list), $result);
    }

    public function test_normalize_null_value() : void
    {
        $entry = new DateTimeEntry('entry', null);

        $result = $this->normalizer->normalize($entry);

        self::assertSame('', $result);
    }

    public function test_normalize_time_entry() : void
    {
        $time = new \DateInterval('PT1H');
        $entry = time_entry('entry', $time);

        $result = $this->normalizer->normalize($entry);

        self::assertEquals('01:00:00', $result);
    }

    public function test_normalize_uuid_entry() : void
    {
        $uuid = '550e8400-e29b-41d4-a716-446655440000';
        $entry = uuid_entry('entry', $uuid);

        $result = $this->normalizer->normalize($entry);

        self::assertEquals($uuid, $result);
    }
}
