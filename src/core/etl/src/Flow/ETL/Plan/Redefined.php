<?php

declare(strict_types=1);

namespace Flow\ETL\Plan;

use Flow\ETL\Row\References;

use function array_intersect;
use function array_values;

final readonly class Redefined
{
    /**
     * @param list<string> $names
     */
    private function __construct(
        public bool $unknown,
        public array $names,
    ) {}

    public static function none(): self
    {
        return new self(false, []);
    }

    public static function names(string ...$names): self
    {
        return new self(false, array_values($names));
    }

    /**
     * The node cannot name its columns before a schema is bound (RenameEach): every reference overlaps.
     */
    public static function unknown(): self
    {
        return new self(true, []);
    }

    public function overlaps(References $refs): bool
    {
        return $this->unknown || array_intersect($this->names, $refs->names()) !== [];
    }
}
