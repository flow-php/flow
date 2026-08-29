<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\SFTP\DirectoryTraversal;

use DateTimeImmutable;

final readonly class RemoteEntry
{
    public function __construct(
        public string $name,
        public RemoteEntryType $type,
        public ?int $size = null,
        public ?DateTimeImmutable $lastModifiedAt = null,
    ) {}

    public function isDirectory(): bool
    {
        return $this->type === RemoteEntryType::DIRECTORY;
    }
}
