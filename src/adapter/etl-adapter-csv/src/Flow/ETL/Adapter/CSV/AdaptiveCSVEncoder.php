<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

use Flow\ETL\FlowPhpExtension;
use Flow\ETL\Rows;

final readonly class AdaptiveCSVEncoder implements CSVEncoder
{
    private CSVEncoder $encoder;

    public function __construct(CSVWriteOptions $options, ?FlowPhpExtension $extension = null)
    {
        $php = new PhpCSVEncoder($options);

        $this->encoder = ($extension ?? FlowPhpExtension::detect())->available()
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
