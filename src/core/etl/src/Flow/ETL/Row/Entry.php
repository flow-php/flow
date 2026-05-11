<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\ETL\Schema\Definition;
use Flow\Types\Type;

/**
 * @template-covariant T
 */
interface Entry extends \Stringable
{
    public function __toString(): string;

    /**
     * @return Definition<T>
     */
    public function definition(): Definition;

    /**
     * @return static
     */
    public function duplicate(): static;

    public function is(string|Reference $name): bool;

    /**
     * @param Entry<mixed> $entry
     */
    public function isEqual(self $entry): bool;

    /**
     * @return static
     */
    public function map(callable $mapper): static;

    public function name(): string;

    public function ref(): Reference;

    /**
     * @return static
     */
    public function rename(string $name): static;

    public function toString(): string;

    /**
     * @return Type<T>
     */
    public function type(): Type;

    /**
     * @return T
     */
    public function value();

    /**
     * @return static
     */
    public function withValue(mixed $value): static;
}
