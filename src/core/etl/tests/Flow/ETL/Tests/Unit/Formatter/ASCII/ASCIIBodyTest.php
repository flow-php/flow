<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Formatter\ASCII;

use function Flow\ETL\DSL\{float_entry, int_entry, ref, row, rows, string_entry};
use Flow\ETL\Formatter\ASCII\{ASCIIBody, Body, Headers};
use Flow\ETL\Tests\FlowTestCase;

final class ASCIIBodyTest extends FlowTestCase
{
    public function test_printing_ascii_body() : void
    {
        $rows = rows(
            row(int_entry('id', 1), float_entry('value', 1.4)),
            row(int_entry('id', 2), float_entry('value', 3.4))
        );

        $headers = new ASCIIBody(
            new Headers($rows),
            new Body($rows)
        );

        self::assertStringContainsString(
            <<<'TABLE'
|  1 | 1.400000 |
|  2 | 3.400000 |
+----+----------+
TABLE,
            $headers->print(false)
        );
    }

    public function test_printing_ascii_body_with_partitioned_rows() : void
    {
        $rows = rows(
            row(int_entry('id', 1), float_entry('value', 1.4), string_entry('group', 'a')),
            row(int_entry('id', 2), float_entry('value', 3.4), string_entry('group', 'a'))
        )->partitionBy(ref('group'));

        $headers = new ASCIIBody(
            new Headers($rows[0]),
            new Body($rows[0])
        );

        self::assertStringContainsString(
            <<<'TABLE'
|  1 | 1.400000 |     a |
|  2 | 3.400000 |     a |
+----+----------+-------+
Partitions:
 - group=a
TABLE,
            $headers->print(false)
        );
    }
}
