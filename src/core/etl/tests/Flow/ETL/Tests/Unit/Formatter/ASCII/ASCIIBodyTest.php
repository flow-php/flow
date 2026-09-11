<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Formatter\ASCII;

use Flow\ETL\Formatter\ASCII\ASCIIBody;
use Flow\ETL\Formatter\ASCII\Body;
use Flow\ETL\Formatter\ASCII\Headers;
use Flow\ETL\Tests\CommandOutputNormalizer;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;

final class ASCIIBodyTest extends FlowTestCase
{
    use CommandOutputNormalizer;

    public function test_printing_ascii_body(): void
    {
        $rows = rows(
            schema(int_schema('id'), float_schema('value')),
            row(['id' => 1, 'value' => 1.4]),
            row(['id' => 2, 'value' => 3.4]),
        );

        $headers = new ASCIIBody(new Headers($rows), new Body($rows));

        self::assertCommandOutputContains(<<<'TABLE'
            |  1 | 1.400000 |
            |  2 | 3.400000 |
            +----+----------+
            TABLE, $headers->print(false));
    }
}
