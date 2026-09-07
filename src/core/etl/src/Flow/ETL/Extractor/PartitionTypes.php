<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\Types\Type;

use function array_key_exists;
use function array_keys;
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
