<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\DSL\Entry;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

/**
 * @implements Transformer<array{algorithm: string, entries: array<string>, new_entry_name: string}>
 * @psalm-immutable
 */
final class HashTransformer implements Transformer
{
    private string $algorithm;

    /**
     * @var array<string>
     */
    private array $entries;

    private string $newEntryName;

    /**
     * @psalm-suppress ImpureFunctionCall
     *
     * @param array<string> $entries
     * @param string $algorithm
     * @param string $newEntryName
     *
     * @throws InvalidArgumentException
     */
    public function __construct(array $entries, string $algorithm, string $newEntryName = 'hash')
    {
        if (!\in_array($algorithm, \hash_algos(), true)) {
            throw new InvalidArgumentException("Unexpected hash algorithm: {$algorithm}");
        }

        $this->algorithm = $algorithm;
        $this->entries = $entries;
        $this->newEntryName = $newEntryName;
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

    public function transform(Rows $rows) : Rows
    {
        /**
         * @psalm-var pure-callable(Row $row) : Row $transformer
         */
        $transformer = function (Row $row) : Row {
            $values = [];

            foreach ($this->entries as $entry) {
                try {
                    $values[] = $row->entries()->get($entry)->toString();
                } catch (InvalidArgumentException $e) {
                    // entry not found, ignore
                }
            }

            return $row->add(Entry::string($this->newEntryName, \hash($this->algorithm, \implode('', $values), false)));
        };

        return $rows->map($transformer);
    }
}
