<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\IO\Extractor;

use Flow\ETL\Adapter\CSV\IO\CSVDialect;

use function array_map;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;

final class CSVDialects
{
    /**
     * @var array<string, CSVDialect>
     */
    private array $dialects = [];

    /**
     * @param array<array-key, mixed> $data the shape of normalize()
     */
    public static function fromArray(array $data): self
    {
        $dialects = new self();

        foreach (type_map(type_string(), type_array())->assert($data) as $uri => $dialect) {
            $dialects->learn($uri, CSVDialect::fromArray($dialect));
        }

        return $dialects;
    }

    public function for(string $uri): ?CSVDialect
    {
        return $this->dialects[$uri] ?? null;
    }

    public function learn(string $uri, CSVDialect $dialect): void
    {
        $this->dialects[$uri] = $dialect;
    }

    /**
     * @return array<string, array{separator: string, enclosure: string, escape: string}>
     */
    public function normalize(): array
    {
        return array_map(static fn(CSVDialect $dialect): array => $dialect->normalize(), $this->dialects);
    }
}
