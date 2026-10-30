<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Unit\IO;

use Flow\ETL\Adapter\CSV\IO\CSVDialect;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\Types\Exception\InvalidTypeException;
use PHPUnit\Framework\TestCase;

final class CSVDialectTest extends TestCase
{
    public function test_a_separator_of_more_than_one_character_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Separator must be a single character');

        new CSVDialect('||', '"', '\\');
    }

    public function test_normalize_round_trips(): void
    {
        $dialect = new CSVDialect(';', "'", '/');

        static::assertSame(['separator' => ';', 'enclosure' => "'", 'escape' => '/'], $dialect->normalize());
        static::assertEquals($dialect, CSVDialect::fromArray($dialect->normalize()));
    }

    public function test_from_array_refuses_a_missing_key(): void
    {
        $this->expectException(InvalidTypeException::class);

        CSVDialect::fromArray(['separator' => ';', 'enclosure' => '"']);
    }

    public function test_the_three_knobs_are_carried_together(): void
    {
        $dialect = new CSVDialect(';', "'", '/');

        static::assertSame(';', $dialect->separator);
        static::assertSame("'", $dialect->enclosure);
        static::assertSame('/', $dialect->escape);
    }
}
