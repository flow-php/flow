<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Unit;

use function Flow\ETL\DSL\{bool_entry,
    date_entry,
    datetime_entry,
    float_entry,
    int_entry,
    null_entry,
    str_entry,
    time_entry,
    uuid_entry};
use Flow\ETL\Adapter\JSON\RowsNormalizer\EntryNormalizer;
use Flow\ETL\Row\Entry;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\DataProvider;

final class EntryNormalizerTest extends FlowTestCase
{
    public static function entries_provider() : \Generator
    {
        yield 'string' => [str_entry('string', 'value'), 'value'];
        yield 'string_nullable' => [str_entry('string', null), null];
        yield 'int' => [int_entry('integer', 1), 1];
        yield 'int_nullable' => [int_entry('integer', null), null];
        yield 'float' => [float_entry('float', 1.1), 1.1];
        yield 'float_nullable' => [float_entry('float', null), null];
        yield 'bool' => [bool_entry('bool', true), 'true'];
        yield 'bool_nullable' => [bool_entry('bool', null), null];
        yield 'null' => [null_entry('null'), null];
        yield 'date' => [date_entry('date', new \DateTimeImmutable('2023-10-01 12:02:01')), '2023-10-01'];
        yield 'date_nullable' => [date_entry('date', null), null];
        yield 'datetime' => [datetime_entry('datetime', new \DateTimeImmutable('2023-10-01 12:02:01')), '2023-10-01T12:02:01+00:00'];
        yield 'datetime_nullable' => [datetime_entry('datetime', null), null];
        yield 'time' => [time_entry('time', new \DateInterval('PT1H')), 3600000000];
        yield 'time_nullable' => [time_entry('time', null), null];
        yield 'uuid' => [uuid_entry('uuid', 'f47ac10b-58cc-4372-a567-0e02b2c3d479'), 'f47ac10b-58cc-4372-a567-0e02b2c3d479'];
        yield 'uuid_nullable' => [uuid_entry('uuid', null), null];
    }

    /**
     * @param Entry<mixed, mixed> $entry
     */
    #[DataProvider('entries_provider')]
    public function test_normalizing_entries(Entry $entry, mixed $expected) : void
    {
        self::assertEquals($expected, (new EntryNormalizer())->normalize($entry));
    }
}
