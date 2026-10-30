<?php

declare(strict_types=1);

namespace Flow\ETL\Foundation\Describe;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\Filesystem\Filesystem;
use ReflectionClass;
use SensitiveParameter;

use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_string;
use function is_a;

/**
 * A filesystem as its class plus the configuration it normalizes to; always secret, credentials live there.
 */
final readonly class ConfiguredFilesystem implements Describable
{
    public function __construct(
        private Filesystem $filesystem,
    ) {}

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        $class = type_string()->assert($data['filesystem'] ?? null);

        if (!is_a($class, Filesystem::class, true) || (new ReflectionClass($class))->isAbstract()) {
            throw InvalidArgumentException::because('%s is not a concrete Filesystem', $class);
        }

        // @mago-ignore analysis:possibly-static-access-on-interface
        return new self($class::fromArray(type_array()->assert($data['configuration'] ?? null)));
    }

    public function describe(): ?Description
    {
        $configuration = $this->filesystem->normalize();

        if ($configuration === null) {
            return null;
        }

        return new Description(
            self::class,
            ['filesystem' => $this->filesystem::class, 'configuration' => $configuration],
            secret: true,
        );
    }

    public function filesystem(): Filesystem
    {
        return $this->filesystem;
    }
}
