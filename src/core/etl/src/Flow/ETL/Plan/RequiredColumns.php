<?php

declare(strict_types=1);

namespace Flow\ETL\Plan;

use function array_diff;
use function array_intersect;
use function array_unique;
use function array_values;
use function in_array;

final readonly class RequiredColumns
{
    /**
     * @param list<string> $names allBut: the columns NOT required; otherwise: the only columns required
     */
    private function __construct(
        public bool $allBut,
        public array $names,
    ) {}

    public static function all(): self
    {
        return new self(true, []);
    }

    public static function allBut(string ...$names): self
    {
        return new self(true, array_values(array_unique($names)));
    }

    public static function only(string ...$names): self
    {
        return new self(false, array_values(array_unique($names)));
    }

    public function isAll(): bool
    {
        return $this->allBut && $this->names === [];
    }

    public function requires(string $name): bool
    {
        return $this->allBut !== in_array($name, $this->names, true);
    }

    /**
     * What either of two consumers reads.
     */
    public function union(self $other): self
    {
        return match (true) {
            !$this->allBut && !$other->allBut => self::only(...$this->names, ...$other->names),
            $this->allBut && $other->allBut => self::allBut(...array_intersect($this->names, $other->names)),
            $this->allBut => self::allBut(...array_diff($this->names, $other->names)),
            default => self::allBut(...array_diff($other->names, $this->names)),
        };
    }

    public function with(string ...$names): self
    {
        return $this->allBut
            ? self::allBut(...array_diff($this->names, $names))
            : self::only(...$this->names, ...$names);
    }

    public function without(string ...$names): self
    {
        return $this->allBut
            ? self::allBut(...$this->names, ...$names)
            : self::only(...array_diff($this->names, $names));
    }
}
