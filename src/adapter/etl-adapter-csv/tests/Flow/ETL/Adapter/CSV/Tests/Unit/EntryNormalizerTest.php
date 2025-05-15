<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Unit;

use function Flow\ETL\DSL\{bool_entry,
    date_entry,
    datetime_entry,
    float_entry,
    int_entry,
    null_entry,
    str_entry,
    time_entry,
    uuid_entry};
use Flow\ETL\Adapter\CSV\RowsNormalizer\EntryNormalizer;
use Flow\ETL\Row\Entry;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\DataProvider;

final class EntryNormalizerTest extends FlowTestCase
{
    public static function entries_provider() : \Generator
    {
        yield 'string' => [str_entry('string', 'value'), 'value'];
        yield 'int' => [int_entry('integer', 1), 1];
        yield 'float' => [float_entry('float', 1.1), 1.1];
        yield 'bool' => [bool_entry('bool', true), 'true'];
        yield 'null' => [null_entry('null'), null];
        yield 'date' => [date_entry('date', new \DateTimeImmutable('2023-10-01 12:02:01')), '2023-10-01'];
        yield 'datetime' => [datetime_entry('datetime', new \DateTimeImmutable('2023-10-01 12:02:01')), '2023-10-01T12:02:01+00:00'];
        yield 'time' => [time_entry('time', new \DateInterval('PT1H')), 3600000000];
        yield 'uuid' => [uuid_entry('uuid', 'f47ac10b-58cc-4372-a567-0e02b2c3d479'), 'f47ac10b-58cc-4372-a567-0e02b2c3d479'];
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
