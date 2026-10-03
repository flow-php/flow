<?php

declare(strict_types=1);

namespace Flow\ETL\Column;

use Flow\ETL\Schema\Definition;
use Flow\Types\Type;

use function interface_exists;

if (interface_exists(Column::class, false)) {
    return;
}

interface Column
{
    /**
     * The logical type; a child column has a Type, never a Definition.
     *
     * @return Type<mixed>
     */
    public function type(): Type;

    public function count(): int;

    public function isNull(int $i): bool;

    public function nullCount(): int;

    /**
     * PHYSICAL cell: int|float|bool|string|array|null - never a DateTimeImmutable, Uuid, Json …
     */
    public function at(int $i): mixed;

    /**
     * @return list<mixed> physical, whole column - PHP returns its backing list (refcount), native copies once
     */
    public function physicals(): array;

    /**
     * LOGICAL cell - PHP: Physical::fromPhysical(at($i)); native: built in Rust.
     */
    public function value(int $i): mixed;

    /**
     * @return list<mixed> logical, whole column (encoders, statistics, reduceToArray)
     */
    public function values(): array;

    /**
     * `self`, never `static`: the extension declares these too and ext-php-rs has no `static` return binding.
     */
    public function slice(int $offset, int $length): self;

    /**
     * @param list<int> $indices every index in [0, count())
     */
    public function take(array $indices): self;

    /**
     * The one type-changing operation: same buffers, a new Type of the same column kind, checked recursively.
     * Dropping an OptionalType or an element's optionality at any depth requires that child column's
     * nullCount() === 0.
     *
     * @param Type<mixed> $type
     */
    public function withType(Type $type): self;

    /**
     * Same-class lane; a mixed-class concat never reaches here - Rows::concat routes it through PhpBackend.
     */
    public function concat(self ...$others): self;

    /**
     * @return list<string> the column's frame buffers in the kind's buffer order, pre-order for nested kinds, in
     *                      canonical form; an omitted validity buffer is ''
     */
    public function encode(): array;
}
