<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON;

use DateTimeInterface;
use Flow\ETL\Rows;

use function extension_loaded;

use const JSON_PRESERVE_ZERO_FRACTION;
use const JSON_THROW_ON_ERROR;
use const JSON_UNESCAPED_SLASHES;
use const JSON_UNESCAPED_UNICODE;

final readonly class AdaptiveJSONEncoder implements JSONEncoder
{
    private JSONEncoder $encoder;

    public function __construct(
        int $flags = JSON_THROW_ON_ERROR,
        string $dateTimeFormat = DateTimeInterface::ATOM,
        string $dateFormat = 'Y-m-d',
    ) {
        $php = new PhpJSONEncoder($flags, $dateTimeFormat, $dateFormat);

        // RustJSONEncoder renders only these flags; any other one writes through PHP
        $this->encoder = extension_loaded('flow_php')
        && (
            $flags
            & ~(JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE | JSON_PRESERVE_ZERO_FRACTION)
        )
            === 0
            ? new RustJSONEncoder($flags, $dateTimeFormat, $dateFormat, $php)
            : $php;
    }

    public function encode(Rows $rows, string $separator): string
    {
        return $this->encoder->encode($rows, $separator);
    }
}
