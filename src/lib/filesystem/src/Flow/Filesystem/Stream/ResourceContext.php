<?php

declare(strict_types=1);

namespace Flow\Filesystem\Stream;

use Flow\Filesystem\Path;

final readonly class ResourceContext
{
    /**
     * @param array<mixed> $options
     */
    private function __construct(private string $scheme, private array $options)
    {
    }

    public static function from(Path $path) : self
    {
        return new self($path->protocol()->scheme(), $path->options()->toArray());
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
