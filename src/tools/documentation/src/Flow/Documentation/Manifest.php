<?php

declare(strict_types=1);

namespace Flow\Documentation;

use Flow\Documentation\Manifest\{Package, Type};

final readonly class Manifest
{
    /**
     * @var array<Package>
     */
    public array $packages;

    public function __construct(Package ...$packages)
    {
        $this->packages = $packages;
    }

    public static function fromJson(string $json) : self
    {
        return new self(...\array_map(
            static fn (array $package) => new Package($package['name'], $package['path'], Type::from($package['type'])),
            \json_decode($json, true, 512, \JSON_THROW_ON_ERROR)['packages']
        ));
    }
}
