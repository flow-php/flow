<?php

declare(strict_types=1);

namespace Flow\Documentation;

use Flow\Documentation\Manifest\Package;
use Flow\Documentation\Manifest\Type;

use function array_map;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_string;
use function json_decode;

use const JSON_THROW_ON_ERROR;

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

    public static function fromJson(string $json): self
    {
        $data = json_decode($json, true, 512, JSON_THROW_ON_ERROR);
        $data = type_array()->assert($data);
        $data['packages'] = type_array()->assert($data['packages']);

        return new self(...array_map(static function (mixed $package): Package {
            $package = type_array()->assert($package);
            $package['name'] = type_string()->assert($package['name']);
            $package['path'] = type_string()->assert($package['path']);

            $package['type'] = type_string()->assert($package['type']);

            return new Package($package['name'], $package['path'], Type::from($package['type']));
        }, $data['packages']));
    }
}
