<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\SFTP;

use DateTimeImmutable;

final readonly class DirectoryEntry
{
    private function __construct(
        public string $path,
        public bool $isDirectory,
        public ?int $size,
        public ?DateTimeImmutable $modifiedAt,
    ) {}

    public static function file(string $path, ?int $size, ?DateTimeImmutable $modifiedAt): self
    {
        return new self($path, false, $size, $modifiedAt);
    }

    public static function subdirectory(string $path): self
    {
        return new self($path, true, null, null);
    }
}
