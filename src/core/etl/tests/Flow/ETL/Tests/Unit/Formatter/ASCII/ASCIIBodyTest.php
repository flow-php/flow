<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Formatter\ASCII;

use function Flow\ETL\DSL\float_entry;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\string_entry;
use Flow\ETL\Formatter\ASCII\ASCIIBody;
use Flow\ETL\Formatter\ASCII\Body;
use Flow\ETL\Formatter\ASCII\Headers;
use PHPUnit\Framework\TestCase;

final class ASCIIBodyTest extends TestCase
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

        $this->assertStringContainsString(
            <<<'TABLE'
|  1 |   1.4 |
|  2 |   3.4 |
+----+-------+
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

        $this->assertStringContainsString(
            <<<'TABLE'
|  1 |   1.4 |     a |
|  2 |   3.4 |     a |
+----+-------+-------+
Partitions:
 - group=a
TABLE,
            $headers->print(false)
        );
    }
}
