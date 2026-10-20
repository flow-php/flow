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
        private Redefinition $redefinition,
        public array $names,
        private array $aliases,
    ) {}

    public static function none(): self
    {
        return new self(Redefinition::names, [], []);
    }

    public static function names(string ...$names): self
    {
        return new self(Redefinition::names, array_values($names), []);
    }

    /**
     * The node keeps only $kept (Select): every other column is gone above it.
     */
    public static function allBut(string ...$kept): self
    {
        return new self(Redefinition::allBut, array_values($kept), []);
    }

    /**
     * $name holds exactly the values of $below: a reference to $name above the node is a reference to $below
     * under it.
     */
    public static function alias(string $name, string $below): self
    {
        return new self(Redefinition::names, [$name], [$name => $below]);
    }

    /**
     * $to holds exactly the values of $from, which is gone above the node.
     */
    public static function rename(string $from, string $to): self
    {
        return new self(Redefinition::names, [$to, $from], [$to => $from]);
    }

    /**
     * The node cannot name its columns before a schema is bound (RenameEach): every name is redefined.
     */
    public static function unknown(): self
    {
        return new self(Redefinition::unknown, [], []);
    }

    public function defines(string $name): bool
    {
        return match ($this->redefinition) {
            Redefinition::unknown => true,
            Redefinition::allBut => !in_array($name, $this->names, true),
            Redefinition::names => in_array($name, $this->names, true),
        };
    }

    public function isAllBut(): bool
    {
        return $this->redefinition === Redefinition::allBut;
    }

    public function isUnknown(): bool
    {
        return $this->redefinition === Redefinition::unknown;
    }

    /**
     * The column under the node that $name above it renames, or null when $name is not a plain alias.
     */
    public function aliasOf(string $name): ?string
    {
        return array_key_exists($name, $this->aliases) ? $this->aliases[$name] : null;
    }
}
