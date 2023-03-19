<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\DSL\Entry;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

/**
 * @implements Transformer<array{algorithm: string, entries: array<string>, new_entry_name: string}>
 */
final class HashTransformer implements Transformer
{
    /**
     * @psalm-suppress ImpureFunctionCall
     *
     * @param array<string> $entries
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        private readonly array $entries,
        private readonly string $algorithm,
        private readonly string $newEntryName = 'hash'
    ) {
        if (!\in_array($algorithm, \hash_algos(), true)) {
            throw new InvalidArgumentException("Unexpected hash algorithm: {$algorithm}");
        }
    }

    public function __serialize() : array
    {
        return [
            'algorithm' => $this->algorithm,
            'entries' => $this->entries,
            'new_entry_name' => $this->newEntryName,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->algorithm = $data['algorithm'];
        $this->entries = $data['entries'];
        $this->newEntryName = $data['new_entry_name'];
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        $transformer = function (Row $row) : Row {
            $values = [];

            foreach ($this->entries as $entry) {
                try {
                    $values[] = $row->entries()->get($entry)->toString();
                } catch (InvalidArgumentException) {
                    // entry not found, ignore
                }
            }

            return $row->set(Entry::string($this->newEntryName, \hash($this->algorithm, \implode('', $values), false)));
        };

        return $rows->map($transformer);
    }
}
