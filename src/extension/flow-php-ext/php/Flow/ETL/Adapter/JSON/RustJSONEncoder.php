<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Rows;
use RuntimeException;

use function extension_loaded;

if (extension_loaded('flow_php')) {
    return;
}

final class RustJSONEncoder implements JSONEncoder
{
    /**
     * @param PhpJSONEncoder $php renders the columns this writer does not render itself
     *
     * @throws InvalidArgumentException a flag outside JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE | JSON_PRESERVE_ZERO_FRACTION
     */
    public function __construct(int $flags, string $dateTimeFormat, string $dateFormat, PhpJSONEncoder $php)
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    public function encode(Rows $rows, string $separator): string
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }
}
