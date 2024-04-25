<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline\HashJoin;

use Flow\ETL\Hash\Algorithm;
use Flow\ETL\Row\References;
use Flow\ETL\{Row, Rows};

final class HashTable
{
    /**
     * @var array<string, Bucket>
     */
    private array $buckets;

    /**
     * @var array<string, int>
     */
    private array $bucketsMatches;

    public function __construct(private readonly Algorithm $hashAlgorithm)
    {
        $this->buckets = [];
        $this->bucketsMatches = [];
    }

    public function add(Row $row, References $hashBy) : void
    {
        $hash = $this->hash($hashBy, $row);

        if (!\array_key_exists($hash, $this->buckets)) {
            $this->buckets[$hash] = new Bucket($hash);
            $this->bucketsMatches[$hash] = 0;
        }

        $this->buckets[$hash]->add($row);
    }

    public function bucketFor(Row $row, References $hashBy) : ?Bucket
    {
        $hash = $this->hash($hashBy, $row);

        if (!\array_key_exists($hash, $this->buckets)) {
            return null;
        }

        $this->bucketsMatches[$hash]++;

        return $this->buckets[$hash];
    }

    public function unmatchedRows() : Rows
    {
        $rows = [];

        foreach ($this->buckets as $hash => $bucket) {
            if ($this->bucketsMatches[$hash] === 0) {
                $rows = \array_merge($rows, $bucket->unmatchedRows());
            }
        }

        return new Rows(...$rows);
    }

    private function hash(References $hashBy, Row $row) : string
    {
        $value = '';

        foreach ($hashBy->all() as $reference) {
            $value .= $row->get($reference)->toString();
        }

        return $this->hashAlgorithm->hash($value);
    }
}
