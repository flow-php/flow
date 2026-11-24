<?php

declare(strict_types=1);

namespace Flow\ETL\Values\Schema;

use function array_unique;
use function sort;

use const SORT_STRING;

final readonly class FingerprintOptions
{
    /**
     * @var list<string>
     */
    public array $metadataKeys;

    /**
     * @param array<array-key, string> $metadataKeys
     */
    public function __construct(array $metadataKeys = [])
    {
        $keys = array_unique($metadataKeys);
        sort($keys, SORT_STRING);

        $this->metadataKeys = $keys;
    }

    public function withMetadataKeys(string ...$keys): self
    {
        return new self($keys);
    }
}
