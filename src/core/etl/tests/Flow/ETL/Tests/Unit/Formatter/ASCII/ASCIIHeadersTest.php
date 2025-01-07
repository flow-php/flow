<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Formatter\ASCII;

use function Flow\ETL\DSL\{float_entry, int_entry};
use function Flow\ETL\DSL\{row, rows};
use Flow\ETL\Formatter\ASCII\{ASCIIHeaders, Body, Headers};
use Flow\ETL\{Tests\FlowTestCase};

final class ASCIIHeadersTest extends FlowTestCase
{
    public function test_printing_ascii_headers() : void
    {
        $rows = rows(row(int_entry('id', 1), float_entry('value', 1.4)), row(int_entry('id', 2), float_entry('value', 3.4)));

        $headers = new ASCIIHeaders(
            new Headers($rows),
            new Body($rows)
        );

        self::assertStringContainsString(
            <<<'TABLE'
+----+----------+
| id |    value |
+----+----------+
TABLE,
            $headers->print(false)
        );
    }
}
