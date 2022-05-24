<?php

declare(strict_types=1);

namespace Flow\ETL\Stream;

use Flow\ETL\Exception\RuntimeException;

final class Handler
{
    private function __construct(
        private readonly bool $safeMode,
        private readonly ?string $extension
    ) {
    }

    public static function directory(string $extension) : self
    {
        return new self(true, $extension);
    }

    public static function file() : self
    {
        return new self(false, null);
    }

    /**
     * @param FileStream $stream
     *
     * @return resource
     */
    public function open(FileStream $stream, Mode $mode)
    {
        /** @psalm-suppress PossiblyNullOperand */
        $fullPath = ($this->safeMode)
            ? (\rtrim($stream->uri(), DIRECTORY_SEPARATOR) . DIRECTORY_SEPARATOR . \uniqid() . '.' . $this->extension)
            : $stream->uri();

        $context = \count($stream->options())
            ? \stream_context_create([$stream->scheme() => $stream->options()])
            : null;

        if ($this->safeMode) {
            $context
                ? \mkdir(\rtrim($stream->uri(), DIRECTORY_SEPARATOR), 0777, false, $context)
                : \mkdir(\rtrim($stream->uri(), DIRECTORY_SEPARATOR));
        }

        $resource = $context
            ? \fopen($fullPath, $mode->value, false, $context)
            : \fopen($fullPath, $mode->value);

        if ($resource === false) {
            throw new RuntimeException("Unable to open stream for path {$stream->uri()}.");
        }

        return $resource;
    }
}
