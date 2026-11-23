<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime\Metrics;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Values\Column\MultisetHashAlgorithm;
use Flow\Filesystem\Partition;

use function array_key_exists;
use function array_map;
use function array_values;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;
use function json_encode;
use function sprintf;

use const JSON_THROW_ON_ERROR;

final readonly class DatasetHash
{
    /**
     * @param list<PartitionHash> $partitions
     */
    public function __construct(
        public Hash $total,
        public array $partitions,
        public MultisetHashAlgorithm $algorithm,
    ) {}

    /**
     * @param array<array-key, mixed> $data
     *
     * @throws InvalidArgumentException
     */
    public static function fromArray(array $data): self
    {
        foreach ([
            'algorithm' => type_string(),
            'total' => type_array(),
            'partitions' => type_list(type_array()),
        ] as $key => $type) {
            if (!array_key_exists($key, $data) || !$type->isValid($data[$key])) {
                throw new InvalidArgumentException(sprintf(
                    'A dataset hash needs "%s" of type %s',
                    $key,
                    $type->toString(),
                ));
            }
        }

        /** @var array{algorithm: string, total: array<array-key, mixed>, partitions: list<array<array-key, mixed>>} $data */
        return new self(
            Hash::fromArray($data['total']),
            array_map(PartitionHash::fromArray(...), $data['partitions']),
            MultisetHashAlgorithm::named($data['algorithm']),
        );
    }

    /**
     * @throws InvalidArgumentException
     */
    public function plus(self $other): self
    {
        if ($this->algorithm !== $other->algorithm) {
            throw new InvalidArgumentException(sprintf(
                'A %s dataset hash cannot be added to a %s dataset hash, hash both datasets with the same algorithm',
                $other->algorithm->value,
                $this->algorithm->value,
            ));
        }

        $partitions = [];

        foreach ([...$this->partitions, ...$other->partitions] as $partition) {
            // one entry is one folder: null and its folder name are one partition
            $folder = json_encode(
                array_map(
                    static fn(Partition $value): string => $value->value ?? Partition::NULL_VALUE,
                    $partition->partitions->toArray(),
                ),
                JSON_THROW_ON_ERROR,
            );
            $partitions[$folder] = array_key_exists($folder, $partitions)
                ? new PartitionHash(
                    $partitions[$folder]->partitions,
                    $partitions[$folder]->hash->plus($partition->hash),
                )
                : $partition;
        }

        return new self($this->total->plus($other->total), array_values($partitions), $this->algorithm);
    }

    /**
     * @return array{algorithm: string, total: array{rows: int, value: string}, partitions: list<array{partitions: array<string, ?string>, hash: array{rows: int, value: string}}>}
     */
    public function normalize(): array
    {
        return [
            'algorithm' => $this->algorithm->value,
            'total' => $this->total->normalize(),
            'partitions' => array_map(
                static fn(PartitionHash $partition): array => $partition->normalize(),
                $this->partitions,
            ),
        ];
    }
}
