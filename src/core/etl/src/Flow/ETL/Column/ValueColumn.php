<?php

declare(strict_types=1);

namespace Flow\ETL\Column;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\Types\Type;
use Flow\Types\Type\Native\MixedType;

use function array_filter;
use function array_slice;
use function count;
use function Flow\Types\DSL\type_mixed;
use function sprintf;

final readonly class ValueColumn implements Column
{
    /**
     * @param list<mixed> $values logical PHP values, null = null
     */
    public function __construct(
        private array $values,
    ) {}

    public function at(int $i): mixed
    {
        return $this->values[$i];
    }

    public function concat(Column ...$others): Column
    {
        throw new InvalidArgumentException(
            'An untyped function result (mixed) cannot be concatenated; it exists only inside function evaluation',
        );
    }

    public function count(): int
    {
        return count($this->values);
    }

    public function encode(): array
    {
        throw new RuntimeException(
            'An untyped function result (mixed) cannot be encoded; it exists only inside function evaluation',
        );
    }

    public function isNull(int $i): bool
    {
        return $this->values[$i] === null;
    }

    public function nullCount(): int
    {
        return count(array_filter($this->values, static fn(mixed $value): bool => $value === null));
    }

    public function physicals(): array
    {
        return $this->values;
    }

    public function slice(int $offset, int $length): Column
    {
        return new self(array_slice($this->values, $offset, $length));
    }

    public function take(array $indices): Column
    {
        $values = [];

        foreach ($indices as $index) {
            $values[] = $this->values[$index];
        }

        return new self($values);
    }

    /**
     * @return MixedType
     */
    public function type(): Type
    {
        return type_mixed();
    }

    public function value(int $i): mixed
    {
        return $this->values[$i];
    }

    public function values(): array
    {
        return $this->values;
    }

    public function withType(Type $type): Column
    {
        throw new InvalidArgumentException(sprintf(
            'An untyped function result (mixed) cannot be retyped to %s; it exists only inside function evaluation',
            $type->toString(),
        ));
    }
}
