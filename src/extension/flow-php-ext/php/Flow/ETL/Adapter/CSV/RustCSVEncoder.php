<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

use Flow\ETL\Rows;
use RuntimeException;

use function extension_loaded;

if (extension_loaded('flow_php')) {
    return;
}

final class RustCSVEncoder implements CSVEncoder
{
    /**
     * @param PhpCSVEncoder $php renders the columns this writer does not render itself, and the header
     */
    public function __construct(
        string $separator,
        string $enclosure,
        string $escape,
        string $newLineSeparator,
        string $dateTimeFormat,
        string $dateFormat,
        PhpCSVEncoder $php,
    ) {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    public function encode(Rows $rows): string
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    /**
     * @param list<string> $headers
     */
    public function encodeHeader(array $headers): string
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }
}
