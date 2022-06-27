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
        return new self(true, \ltrim($extension, '.'));
    }

    public static function file() : self
    {
        return new self(false, null);
    }

    /**
     * @return resource
     */
    public function open(FileStream $stream, Mode $mode)
    {
        $context = \count($stream->options())
            ? \stream_context_create([$stream->scheme() => $stream->options()])
            : null;

        if ($this->safeMode) {
            $dirname = \rtrim($stream->uri(), DIRECTORY_SEPARATOR);
            $fullPath = $dirname . DIRECTORY_SEPARATOR . \bin2hex(\random_bytes(13)) . '.' . ($this->extension ?: '');

            if ($stream instanceof LocalFile) {
                if (!\file_exists($stream->uri())) {
                    $context
                        ? \mkdir($dirname, 0755, true, $context)
                        : \mkdir($dirname, 0755, true);
                }
            } else {
                $context
                    ? \mkdir($dirname, 0755, true, $context)
                    : \mkdir($dirname, 0755, true);
            }
        } else {
            $fullPath = $stream->uri();
        }

        $resource = \fopen($fullPath, $mode->value, null !== $context, $context);

        if ($resource === false) {
            throw new RuntimeException("Unable to open stream for path {$stream->uri()}.");
        }

        return $resource;
    }
}
