<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Unit;

use Flow\ETL\Adapter\CSV\CSVHeader;
use PHPUnit\Framework\TestCase;

final class CSVHeaderTest extends TestCase
{
    public function test_no_source_resolved_a_header(): void
    {
        $header = new CSVHeader([], null);

        static::assertSame([], $header->names);
        static::assertNull($header->source);
    }

    public function test_the_names_and_the_source_are_carried_together(): void
    {
        $header = new CSVHeader(['id', 'name'], 'file:///a.csv');

        static::assertSame(['id', 'name'], $header->names);
        static::assertSame('file:///a.csv', $header->source);
    }
}
