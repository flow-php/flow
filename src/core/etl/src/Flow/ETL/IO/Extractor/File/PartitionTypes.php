<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Extractor\File;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\Types\Type;
use Flow\Types\Type\TypeFactory;

use function array_key_exists;
use function array_keys;
use function Flow\Types\DSL\type_array;
use function implode;
use function sprintf;

/**
 * Types for partition columns discovered from a path.
 */
final readonly class PartitionTypes
{
    /**
     * @param array<string, Type<mixed>> $types
     */
    public function __construct(
        private array $types = [],
    ) {}

    /**
     * @param array<array-key, mixed> $data the shape of normalize()
     */
    public static function fromArray(array $data): self
    {
        $types = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($data as $name => $type) {
            /** @var array<string, mixed> $type */
            $type = type_array()->assert($type);
            $types[(string) $name] = TypeFactory::fromArray($type);
        }

        return new self($types);
    }

    /**
     * @return Type<mixed>
     */
    public function get(string $name): Type
    {
        if (!array_key_exists($name, $this->types)) {
            throw new InvalidArgumentException(sprintf('No partition type declared for "%s"', $name));
        }

        return $this->types[$name];
    }

    public function has(string $name): bool
    {
        return array_key_exists($name, $this->types);
    }

    /**
     * @return array<string, array<string, mixed>> every type as Type::normalize()
     */
    public function normalize(): array
    {
        $normalized = [];

        foreach ($this->types as $name => $type) {
            $normalized[$name] = $type->normalize();
        }

        return $normalized;
    }

    /**
     * @param array<string, bool> $names discovered partition columns
     */
    public function assertEveryNameIsAPartition(array $names): void
    {
        foreach (array_keys($this->types) as $name) {
            if (!array_key_exists($name, $names)) {
                throw new InvalidArgumentException(sprintf(
                    'Column "%s" is not a partition of this read, discovered partitions: [%s]',
                    $name,
                    $names === [] ? '' : '"' . implode('", "', array_keys($names)) . '"',
                ));
            }
        }
    }
}
