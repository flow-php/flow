<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON;

use Flow\Filesystem\DestinationStream;

use function extension_loaded;

use const JSON_PRESERVE_ZERO_FRACTION;
use const JSON_THROW_ON_ERROR;
use const JSON_UNESCAPED_SLASHES;
use const JSON_UNESCAPED_UNICODE;

final readonly class JsonSinkOpener
{
    public function __construct(
        private int $flags,
        private string $dateTimeFormat,
        private string $dateFormat,
        private JsonFraming $framing,
    ) {}

    public function open(DestinationStream $stream): JsonOpenSink
    {
        $php = new PhpJSONEncoder($this->flags, $this->dateTimeFormat, $this->dateFormat);

        return new JsonOpenSink(
            $stream,
            extension_loaded('flow_php')
            && (
                $this->flags
                & ~(JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE | JSON_PRESERVE_ZERO_FRACTION)
            )
                === 0
                ? new NativeJSONEncoder(
                    new NativeJsonWriter($this->flags, $this->dateTimeFormat, $this->dateFormat),
                    $php,
                )
                : $php,
            $this->framing,
        );
    }
}
