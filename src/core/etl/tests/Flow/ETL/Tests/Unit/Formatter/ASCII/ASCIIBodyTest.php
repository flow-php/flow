<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Formatter\ASCII;

use Flow\ETL\Formatter\ASCII\ASCIIBody;
use Flow\ETL\Formatter\ASCII\Body;
use Flow\ETL\Formatter\ASCII\Headers;
use Flow\ETL\Tests\CommandOutputNormalizer;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\float_entry;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\string_entry;

final class ASCIIBodyTest extends FlowTestCase
{
    use CommandOutputNormalizer;

    public function test_printing_ascii_body(): void
    {
        $rows = rows(
            row(int_entry('id', 1), float_entry('value', 1.4)),
            row(int_entry('id', 2), float_entry('value', 3.4)),
        );

        $headers = new ASCIIBody(new Headers($rows), new Body($rows));

        self::assertCommandOutputContains(<<<'TABLE'
            |  1 | 1.400000 |
            |  2 | 3.400000 |
            +----+----------+
            TABLE, $headers->print(false));
    }

    public function test_printing_ascii_body_with_partitioned_rows(): void
    {
        $rows = rows(
            row(int_entry('id', 1), float_entry('value', 1.4), string_entry('group', 'a')),
            row(int_entry('id', 2), float_entry('value', 3.4), string_entry('group', 'a')),
        )->partitionBy(ref('group'));

        $headers = new ASCIIBody(new Headers($rows[0]), new Body($rows[0]));

        self::assertCommandOutputContains(<<<'TABLE'
            |  1 | 1.400000 |     a |
            |  2 | 3.400000 |     a |
            +----+----------+-------+
            Partitions:
             - group=a
            TABLE, $headers->print(false));
    }
}
