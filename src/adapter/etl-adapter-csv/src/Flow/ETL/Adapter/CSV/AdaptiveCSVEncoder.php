<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

use Flow\ETL\Rows;

use function extension_loaded;

final readonly class AdaptiveCSVEncoder implements CSVEncoder
{
    private CSVEncoder $encoder;

    public function __construct(CSVWriteOptions $options)
    {
        $php = new PhpCSVEncoder($options);

        $this->encoder = extension_loaded('flow_php')
            ? new RustCSVEncoder(
                $options->separator,
                $options->enclosure,
                $options->escape,
                $options->newLineSeparator,
                $options->dateTimeFormat,
                $options->dateFormat,
                $php,
            )
            : $php;
    }

    public function encode(Rows $rows): string
    {
        return $this->encoder->encode($rows);
    }

    public function encodeHeader(array $headers): string
    {
        return $this->encoder->encodeHeader($headers);
    }
}
