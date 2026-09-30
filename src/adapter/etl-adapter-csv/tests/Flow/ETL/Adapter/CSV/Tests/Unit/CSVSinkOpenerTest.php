<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Unit;

use Flow\ETL\Adapter\CSV\CSVSinkOpener;
use Flow\ETL\Adapter\CSV\CSVWriteOptions;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Stream\StringDestinationStream;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\path;

final class CSVSinkOpenerTest extends FlowTestCase
{
    public function test_an_opened_sink_writes_with_the_options_of_its_opener(): void
    {
        $stream = new StringDestinationStream(path('memory://out.csv'));

        (new CSVSinkOpener(new CSVWriteOptions(separator: ';', newLineSeparator: "\n")))
            ->open($stream, true)
            ->write(array_to_rows([['id' => 1, 'name' => 'a;b']], schema(int_schema('id'), str_schema('name'))));

        static::assertSame("id;name\n1;\"a;b\"\n", $stream->content());
    }

    public function test_an_opened_sink_without_a_header(): void
    {
        $stream = new StringDestinationStream(path('memory://out.csv'));

        (new CSVSinkOpener(new CSVWriteOptions(newLineSeparator: "\n")))
            ->open($stream, false)
            ->write(array_to_rows([['id' => 1]], schema(int_schema('id'))));

        static::assertSame("1\n", $stream->content());
    }
}
