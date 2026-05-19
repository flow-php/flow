<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Elasticsearch\EntryIdFactory;

use Flow\ETL\Adapter\Elasticsearch\IdFactory;
use Flow\ETL\Hash\Algorithm;
use Flow\ETL\Hash\NativePHPHash;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;
use Stringable;

use function array_map;
use function Flow\ETL\DSL\string_entry;
use function implode;
use function is_scalar;

final class HashIdFactory implements IdFactory
{
    /**
     * @var array<string>
     */
    private readonly array $entryNames;

    private Algorithm $hashAlgorithm;

    public function __construct(string ...$entryNames)
    {
        $this->entryNames = $entryNames;
        $this->hashAlgorithm = new NativePHPHash();
    }

    public function create(Row $row): Entry
    {
        return string_entry('id', $this->hashAlgorithm->hash(implode(':', array_map(static function (string $name) use (
            $row,
        ): string {
            $value = $row->valueOf($name);

            return is_scalar($value) || $value instanceof Stringable ? (string) $value : '';
        }, $this->entryNames))));
    }

    public function withAlgorithm(Algorithm $algorithm): self
    {
        $factory = new self(...$this->entryNames);
        $factory->hashAlgorithm = $algorithm;

        return $factory;
    }
}
