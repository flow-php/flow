<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Extractor\Grid;

use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;

final readonly class GridHeader
{
    /**
     * $source is a plain label, never a Path or a SourceFile, so a sampler with no file can supply its own.
     * It is null exactly when $names is empty - no listed source resolved a header.
     *
     * @param list<string> $names
     */
    public function __construct(
        public array $names,
        public ?string $source,
    ) {}

    /**
     * @param array<array-key, mixed> $data the shape of normalize()
     */
    public static function fromArray(array $data): self
    {
        return new self(
            type_list(type_string())->assert($data['names'] ?? null),
            type_optional(type_string())->assert($data['source'] ?? null),
        );
    }

    /**
     * @return array{names: list<string>, source: ?string}
     */
    public function normalize(): array
    {
        return ['names' => $this->names, 'source' => $this->source];
    }
}
