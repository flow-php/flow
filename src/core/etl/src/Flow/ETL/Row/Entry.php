<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\ETL\Schema\Definition;
use Flow\Types\Type;
use Stringable;

/**
 * @template-covariant T
 */
interface Entry extends Stringable
{
    public function __toString(): string;

    /**
     * @return Definition<mixed>
     */
    public function definition(): Definition;

    public function is(string|Reference $name): bool;

    /**
     * @param Entry<mixed> $entry
     */
    public function isEqual(self $entry): bool;

    public function name(): string;

    public function ref(): Reference;

    /**
     * @return static<T>
     */
    public function rename(string $name): static;

    public function toString(): string;

    /**
     * @return Type<mixed>
     */
    public function type(): Type;

    /**
     * @return T
     */
    public function value();
}
