<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Filesystem;

use Flow\Bridge\Symfony\FilesystemBundle\Exception\{InvalidArgumentException, LogicException};
use Flow\Filesystem\Protocol;

final class FilesystemFactoryRegistry
{
    /** @var array<string, FilesystemFactory> */
    private array $factories = [];

    /**
     * @param iterable<FilesystemFactory> $factories
     */
    public function __construct(iterable $factories)
    {
        foreach ($factories as $factory) {
            $name = $factory->protocol()->name;

            if (\array_key_exists($name, $this->factories)) {
                throw new LogicException(\sprintf('Duplicate filesystem factory for protocol "%s".', $name));
            }

            $this->factories[$name] = $factory;
        }
    }

    public function get(Protocol $protocol) : FilesystemFactory
    {
        if (!\array_key_exists($protocol->name, $this->factories)) {
            throw new InvalidArgumentException(\sprintf(
                'No filesystem factory registered for protocol "%s". Available protocols: [%s].',
                $protocol->name,
                \implode(', ', \array_keys($this->factories)),
            ));
        }

        return $this->factories[$protocol->name];
    }

    public function has(Protocol $protocol) : bool
    {
        return \array_key_exists($protocol->name, $this->factories);
    }

    /**
     * @return list<string>
     */
    public function protocols() : array
    {
        return \array_keys($this->factories);
    }
}
