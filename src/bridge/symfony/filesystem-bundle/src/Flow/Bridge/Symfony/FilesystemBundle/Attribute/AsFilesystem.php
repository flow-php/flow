<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Attribute;

use Attribute;

#[Attribute(Attribute::TARGET_PARAMETER)]
final readonly class AsFilesystem
{
    /**
     * @param string $mount mount protocol to inject (the YAML key under `filesystems:`)
     * @param null|string $fstab fstab name; defaults to the bundle's resolved default fstab when null
     */
    public function __construct(
        public string $mount,
        public ?string $fstab = null,
    ) {}
}
