<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Column\Column;
use Flow\ETL\Exception\RuntimeException;
use Flow\Types\Type;

use function array_slice;
use function count;

/**
 * A column that hands out exactly the values it was given - the same objects on every read.
 */
final readonly class FixedValuesColumn implements Column
{
    /**
     * @param Type<mixed> $type
     * @param list<mixed> $values
     */
    public function __construct(
        public Type $type,
        public array $values,
    ) {}

    public function at(int $i): mixed
    {
        return $this->values[$i];
    }

    public function concat(Column ...$others): Column
    {
        throw new RuntimeException('FixedValuesColumn cannot be concatenated');
    }

    public function count(): int
    {
        return count($this->values);
    }

    public function encode(): array
    {
        throw new RuntimeException('FixedValuesColumn cannot be encoded');
    }

    public function isNull(int $i): bool
    {
        return $this->values[$i] === null;
    }

    public function nullCount(): int
    {
        return 0;
    }

    public function physicals(): array
    {
        return $this->values;
    }

    public function slice(int $offset, int $length): Column
    {
        return new self($this->type, array_slice($this->values, $offset, $length));
    }

    public function take(array $indices): Column
    {
        $values = [];

        foreach ($indices as $index) {
            $values[] = $this->values[$index];
        }

        return new self($this->type, $values);
    }

    public function type(): Type
    {
        return $this->type;
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
        return new self($type, $this->values);
    }
}
