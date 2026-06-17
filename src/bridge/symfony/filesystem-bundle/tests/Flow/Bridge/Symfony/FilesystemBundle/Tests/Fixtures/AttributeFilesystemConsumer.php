<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Fixtures;

use Flow\Bridge\Symfony\FilesystemBundle\Attribute\AsFilesystem;
use Flow\Filesystem\Filesystem;

final readonly class AttributeFilesystemConsumer
{
    public function __construct(
        #[AsFilesystem('memory')]
        public Filesystem $primary,
        #[AsFilesystem('file', fstab: 'archive')]
        public Filesystem $cold,
    ) {}
}
