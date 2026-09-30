<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

use Flow\Filesystem\DestinationStream;

use function extension_loaded;

final readonly class CSVSinkOpener
{
    public function __construct(
        private CSVWriteOptions $options,
    ) {}

    public function open(DestinationStream $stream, bool $header): CSVOpenSink
    {
        $php = new PhpCSVEncoder($this->options);

        return new CSVOpenSink(
            $stream,
            extension_loaded('flow_php')
                ? new NativeCSVEncoder(
                    new NativeCSVWriter(
                        $this->options->separator,
                        $this->options->enclosure,
                        $this->options->escape,
                        $this->options->newLineSeparator,
                        $this->options->dateTimeFormat,
                        $this->options->dateFormat,
                    ),
                    $php,
                )
                : $php,
            $header,
        );
    }
}
