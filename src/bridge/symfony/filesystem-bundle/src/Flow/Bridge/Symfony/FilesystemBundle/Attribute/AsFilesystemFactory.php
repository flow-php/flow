<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Attribute;

#[\Attribute(\Attribute::TARGET_CLASS)]
final readonly class AsFilesystemFactory
{
    public function __construct(
        public string $type,
    ) {}
}
