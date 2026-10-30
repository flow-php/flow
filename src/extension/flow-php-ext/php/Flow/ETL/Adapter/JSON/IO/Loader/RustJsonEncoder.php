<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\IO\Loader;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Values\Rows;
use RuntimeException;

use function extension_loaded;

if (extension_loaded('flow_php')) {
    return;
}

final class RustJsonEncoder implements JsonEncoder
{
    /**
     * @param PhpJsonEncoder $php renders the columns this writer does not render itself
     *
     * @throws InvalidArgumentException a flag outside JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE | JSON_PRESERVE_ZERO_FRACTION
     */
    public function __construct(PhpJsonEncoder $php)
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    public function encode(Rows $rows, string $separator): string
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }
}
