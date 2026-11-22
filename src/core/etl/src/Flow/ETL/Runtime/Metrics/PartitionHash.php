<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime\Metrics;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\Filesystem\Partition;
use Flow\Filesystem\Partitions;

use function array_key_exists;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function sprintf;

final readonly class PartitionHash
{
    public function __construct(
        public Partitions $partitions,
        public Hash $hash,
    ) {}

    /**
     * @param array<array-key, mixed> $data
     *
     * @throws InvalidArgumentException
     */
    public static function fromArray(array $data): self
    {
        foreach ([
            'partitions' => type_map(type_string(), type_optional(type_string())),
            'hash' => type_array(),
        ] as $key => $type) {
            if (!array_key_exists($key, $data) || !$type->isValid($data[$key])) {
                throw new InvalidArgumentException(sprintf(
                    'A partition hash needs "%s" of type %s',
                    $key,
                    $type->toString(),
                ));
            }
        }

        /** @var array{partitions: array<string, ?string>, hash: array<array-key, mixed>} $data */
        return new self(new Partitions(...Partition::fromArray($data['partitions'])), Hash::fromArray($data['hash']));
    }

    /**
     * @return array{partitions: array<string, ?string>, hash: array{rows: int, value: string}}
     */
    public function normalize(): array
    {
        $partitions = [];

        foreach ($this->partitions->toArray() as $partition) {
            $partitions[$partition->name] = $partition->value;
        }

        return ['partitions' => $partitions, 'hash' => $this->hash->normalize()];
    }
}
