<?php

declare(strict_types=1);

namespace Flow\ETL\Plan;

use function array_key_exists;
use function array_values;
use function in_array;

final readonly class Redefined
{
    /**
     * @param list<string> $names
     * @param array<string, string> $aliases a redefined name => the column below the node it only renames
     */
    private function __construct(
        public bool $unknown,
        public array $names,
        private array $aliases,
    ) {}

    public static function none(): self
    {
        return new self(false, [], []);
    }

    public static function names(string ...$names): self
    {
        return new self(false, array_values($names), []);
    }

    /**
     * $name holds exactly the values of $below: a reference to $name above the node is a reference to $below
     * under it.
     */
    public static function alias(string $name, string $below): self
    {
        return new self(false, [$name], [$name => $below]);
    }

    /**
     * The node cannot name its columns before a schema is bound (RenameEach): every name is redefined.
     */
    public static function unknown(): self
    {
        return new self(true, [], []);
    }

    public function defines(string $name): bool
    {
        return $this->unknown || in_array($name, $this->names, true);
    }

    /**
     * The column under the node that $name above it renames, or null when $name is not a plain alias.
     */
    public function aliasOf(string $name): ?string
    {
        return array_key_exists($name, $this->aliases) ? $this->aliases[$name] : null;
    }
}
