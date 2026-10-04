<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use ArrayObject;
use Flow\ETL\Column\Column;
use Flow\Types\Type;

use function array_map;

/**
 * A column that records every call made to it - shared across the columns it produces, so a whole merge or window
 * is counted.
 */
final readonly class SpyColumn implements Column
{
    /**
     * @param ArrayObject<int, string> $calls one method name per call
     */
    public function __construct(
        public Column $inner,
        public ArrayObject $calls = new ArrayObject(),
    ) {}

    public function at(int $i): mixed
    {
        $this->calls->append('at');

        return $this->inner->at($i);
    }

    public function concat(Column ...$others): Column
    {
        $this->calls->append('concat');

        return new self(
            $this->inner->concat(...array_map(static fn(Column $other): Column => $other instanceof self
                ? $other->inner
                : $other, $others)),
            $this->calls,
        );
    }

    public function count(): int
    {
        return $this->inner->count();
    }

    public function encode(): array
    {
        return $this->inner->encode();
    }

    public function isNull(int $i): bool
    {
        return $this->inner->isNull($i);
    }

    public function nullCount(): int
    {
        return $this->inner->nullCount();
    }

    public function physicals(): array
    {
        $this->calls->append('physicals');

        return $this->inner->physicals();
    }

    public function slice(int $offset, int $length): Column
    {
        return new self($this->inner->slice($offset, $length), $this->calls);
    }

    public function take(array $indices): Column
    {
        return new self($this->inner->take($indices), $this->calls);
    }

    /**
     * @return Type<mixed>
     */
    public function type(): Type
    {
        return $this->inner->type();
    }

    public function value(int $i): mixed
    {
        $this->calls->append('value');

        return $this->inner->value($i);
    }

    public function values(): array
    {
        $this->calls->append('values');

        return $this->inner->values();
    }

    public function withType(Type $type): Column
    {
        return new self($this->inner->withType($type), $this->calls);
    }
}
