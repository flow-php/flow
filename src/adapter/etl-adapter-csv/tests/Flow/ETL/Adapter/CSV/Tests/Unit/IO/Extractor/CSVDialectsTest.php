<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Unit\IO\Extractor;

use Flow\ETL\Adapter\CSV\IO\CSVDialect;
use Flow\ETL\Adapter\CSV\IO\Extractor\CSVDialects;
use Flow\Types\Exception\InvalidTypeException;
use PHPUnit\Framework\TestCase;

final class CSVDialectsTest extends TestCase
{
    public function test_a_learned_dialect_is_known_by_its_uri(): void
    {
        $dialects = new CSVDialects();
        $dialects->learn('file:///a.csv', new CSVDialect(';', '"', '\\'));

        static::assertEquals(new CSVDialect(';', '"', '\\'), $dialects->for('file:///a.csv'));
        static::assertNull($dialects->for('file:///b.csv'));
    }

    public function test_normalize_round_trips(): void
    {
        $dialects = new CSVDialects();
        $dialects->learn('file:///a.csv', new CSVDialect(';', '"', '\\'));
        $dialects->learn('file:///b.csv', new CSVDialect(',', "'", ''));

        static::assertSame(
            [
                'file:///a.csv' => ['separator' => ';', 'enclosure' => '"', 'escape' => '\\'],
                'file:///b.csv' => ['separator' => ',', 'enclosure' => "'", 'escape' => ''],
            ],
            $dialects->normalize(),
        );
        static::assertEquals($dialects, CSVDialects::fromArray($dialects->normalize()));
    }

    public function test_from_array_refuses_a_bad_entry(): void
    {
        $this->expectException(InvalidTypeException::class);

        CSVDialects::fromArray(['file:///a.csv' => ['separator' => ';', 'enclosure' => '"']]);
    }
}
