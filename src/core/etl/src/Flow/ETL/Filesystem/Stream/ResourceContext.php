<?php

declare(strict_types=1);

namespace Flow\ETL\Filesystem\Stream;

use Flow\ETL\Filesystem\Path;

final class ResourceContext
{
    /**
     * @param array<mixed> $options
     */
    private function __construct(private readonly string $scheme, private readonly array $options)
    {
    }

    public static function from(Path $path) : self
    {
        return new self($path->scheme(), $path->options());
    }

    /**
     * @return null|resource
     */
    public function resource()
    {
        return \count($this->options)
            ? \stream_context_create([$this->scheme => $this->options])
            : null;
    }
}
