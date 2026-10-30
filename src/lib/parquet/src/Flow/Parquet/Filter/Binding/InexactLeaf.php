<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter\Binding;

use DateTimeImmutable;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;

final readonly class InexactLeaf
{
    /**
     * @param list<string> $fields the struct fields from the root column down to the leaf
     * @param list<bool|DateTimeImmutable|float|int|string> $literals the leaf's literals, normalised by the domain
     */
    public function __construct(
        public FlatColumn $column,
        public LiteralDomain $domain,
        public string $root,
        public array $fields,
        public array $literals,
    ) {}
}
