<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\SFTP;

use Flow\Filesystem\Path;

use function ltrim;
use function rtrim;
use function strrpos;
use function substr;

final readonly class RemotePath
{
    private function __construct(
        private string $path,
    ) {}

    public static function from(Path $path): self
    {
        return new self('/' . ltrim($path->path(), '/'));
    }

    public function directory(): self
    {
        $separator = strrpos($this->path, '/');

        if ($separator === false || $separator === 0) {
            return new self('/');
        }

        return new self(substr($this->path, 0, $separator));
    }

    public function isRoot(): bool
    {
        return $this->path === '/';
    }

    public function toString(): string
    {
        return $this->path;
    }

    public function withoutTrailingSlash(): string
    {
        return rtrim($this->path, '/') ?: '/';
    }
}
