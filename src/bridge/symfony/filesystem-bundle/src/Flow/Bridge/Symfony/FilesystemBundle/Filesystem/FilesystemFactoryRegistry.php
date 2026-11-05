<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Filesystem;

use Flow\Bridge\Symfony\FilesystemBundle\Exception\InvalidArgumentException;
use Flow\Bridge\Symfony\FilesystemBundle\Exception\LogicException;
use Flow\Filesystem\Filesystem;

use function array_key_exists;
use function array_keys;
use function implode;
use function is_string;
use function sprintf;

final class FilesystemFactoryRegistry
{
    /** @var array<string, FilesystemFactory> keyed by factory type */
    private array $factories = [];

    /**
     * @param iterable<FilesystemFactory> $factories
     */
    public function __construct(iterable $factories)
    {
        foreach ($factories as $factory) {
            $type = $factory->type();

            if (array_key_exists($type, $this->factories)) {
                throw new LogicException(sprintf('Duplicate filesystem factory for type "%s".', $type));
            }

            $this->factories[$type] = $factory;
        }
    }

    /**
     * @param array<string, mixed> $entry
     */
    public function create(string $fstabName, string $protocol, array $entry): Filesystem
    {
        $type = is_string($entry['type'] ?? null) ? $entry['type'] : '';
        unset($entry['type']);

        try {
            return $this->get($type)->create($protocol, $entry);
        } catch (InvalidArgumentException $e) {
            throw new LogicException(
                sprintf('Fstab "%s" mount "%s": %s', $fstabName, $protocol, $e->getMessage()),
                0,
                $e,
            );
        }
    }

    public function get(string $type): FilesystemFactory
    {
        if (!array_key_exists($type, $this->factories)) {
            throw new InvalidArgumentException(sprintf(
                'No filesystem factory registered for type "%s". Available types: [%s].',
                $type,
                implode(', ', array_keys($this->factories)),
            ));
        }

        return $this->factories[$type];
    }

    public function has(string $type): bool
    {
        return array_key_exists($type, $this->factories);
    }

    /**
     * @return list<string>
     */
    public function types(): array
    {
        return array_keys($this->factories);
    }
}
