<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Unit;

use Flow\ETL\Adapter\CSV\AdaptiveCSVEncoder;
use Flow\ETL\Adapter\CSV\CSVOpenSink;
use Flow\ETL\Adapter\CSV\CSVWriteOptions;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\FlowPhpExtension;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Stream\StringDestinationStream;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\path;

final class AdaptiveCSVEncoderTest extends FlowTestCase
{
    public function test_a_sink_writes_with_the_options_of_its_encoder(): void
    {
        $stream = new StringDestinationStream(path('memory://out.csv'));

        (new CSVOpenSink(
            $stream,
            new AdaptiveCSVEncoder(new CSVWriteOptions(separator: ';', newLineSeparator: "\n")),
            true,
        ))->write(array_to_rows([['id' => 1, 'name' => 'a;b']], schema(int_schema('id'), str_schema('name'))));

        static::assertSame("id;name\n1;\"a;b\"\n", $stream->content());
    }

    public function test_a_sink_without_a_header(): void
    {
        $stream = new StringDestinationStream(path('memory://out.csv'));

        (new CSVOpenSink(
            $stream,
            new AdaptiveCSVEncoder(new CSVWriteOptions(newLineSeparator: "\n")),
            false,
        ))->write(array_to_rows([['id' => 1]], schema(int_schema('id'))));

        static::assertSame("1\n", $stream->content());
    }

    public function test_a_flow_php_of_another_abi_is_refused(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('does not match flow-php/etl');

        new AdaptiveCSVEncoder(new CSVWriteOptions(), new FlowPhpExtension(true, FlowPhpExtension::ABI + 1, '0.46.0'));
    }

    public function test_without_flow_php_the_php_encoder_writes(): void
    {
        static::assertSame(
            "1\n",
            (new AdaptiveCSVEncoder(
                new CSVWriteOptions(newLineSeparator: "\n"),
                new FlowPhpExtension(false, null),
            ))->encode(array_to_rows([['id' => 1]], schema(int_schema('id')))),
        );
    }
}
