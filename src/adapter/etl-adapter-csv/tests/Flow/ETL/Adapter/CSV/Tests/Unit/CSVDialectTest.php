<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Unit;

use Flow\ETL\Adapter\CSV\CSVDialect;
use PHPUnit\Framework\TestCase;

final class CSVDialectTest extends TestCase
{
    /**
     * A settled dialect is not the detector's scoring candidate: it carries whatever the user pinned, including
     * a multi-character value that Detector\Option's constructor would reject.
     */
    public function test_a_multi_character_value_is_carried_verbatim(): void
    {
        $dialect = new CSVDialect('||', '""', 'ab');

        static::assertSame('||', $dialect->separator);
        static::assertSame('""', $dialect->enclosure);
        static::assertSame('ab', $dialect->escape);
    }

    public function test_the_three_knobs_are_carried_together(): void
    {
        $dialect = new CSVDialect(';', "'", '/');

        static::assertSame(';', $dialect->separator);
        static::assertSame("'", $dialect->enclosure);
        static::assertSame('/', $dialect->escape);
    }
}
