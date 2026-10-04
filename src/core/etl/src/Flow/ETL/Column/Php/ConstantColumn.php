<?php

declare(strict_types=1);

namespace Flow\ETL\Column\Php;

use Flow\ETL\Column\Column;
use Flow\ETL\Column\Physical\Physical;
use Flow\ETL\Column\Physical\PhysicalBuilderFor;
use Flow\ETL\Column\Physical\PhysicalFor;
use Flow\ETL\Column\Retype;
use Flow\Types\Type;
use Flow\Types\Type\Logical\OptionalType;
use Flow\Types\Type\Native\NullType;

use function array_fill;
use function array_map;
use function count;

final readonly class ConstantColumn implements Column
{
    /**
     * @param Type<mixed> $type
     */
    public function __construct(
        public Type $type,
        public Physical $physical,
        public mixed $physicalValue,
        public int $count,
    ) {}

    public function at(int $i): mixed
    {
        return $this->physicalValue;
    }

    public function concat(Column ...$others): Column
    {
        $count = $this->count;

        foreach ($others as $other) {
            if (!$other instanceof self || $other->physicalValue !== $this->physicalValue) {
                return $this->expand()->concat(...array_map(static fn(Column $column): Column => $column instanceof self
                    ? $column->expand()
                    : $column, $others));
            }

            $count += $other->count;
        }

        return new self($this->type, $this->physical, $this->physicalValue, $count);
    }

    public function count(): int
    {
        return $this->count;
    }

    public function encode(): array
    {
        return $this->isNullKind() ? [] : $this->expand()->encode();
    }

    public function expand(): Column
    {
        if ($this->isNullKind()) {
            return $this;
        }

        $builder = (new PhysicalBuilderFor())->type($this->type);
        $builder->appendPhysicals($this->physicals());

        return $builder->finish();
    }

    public function isNull(int $i): bool
    {
        return $this->physicalValue === null;
    }

    public function isNullKind(): bool
    {
        return ($this->type instanceof OptionalType ? $this->type->base() : $this->type) instanceof NullType;
    }

    public function nullCount(): int
    {
        return $this->physicalValue === null ? $this->count : 0;
    }

    public function physicals(): array
    {
        // @mago-ignore analysis:possibly-invalid-argument
        return $this->count === 0 ? [] : array_fill(0, $this->count, $this->physicalValue);
    }

    public function slice(int $offset, int $length): Column
    {
        return new self($this->type, $this->physical, $this->physicalValue, $length);
    }

    public function take(array $indices): Column
    {
        return new self($this->type, $this->physical, $this->physicalValue, count($indices));
    }

    public function type(): Type
    {
        return $this->type;
    }

    public function value(int $i): mixed
    {
        return $this->physicalValue === null ? null : $this->physical->fromPhysical($this->physicalValue);
    }

    public function values(): array
    {
        // @mago-ignore analysis:possibly-invalid-argument
        return $this->count === 0 ? [] : array_fill(0, $this->count, $this->value(0));
    }

    public function withType(Type $type): Column
    {
        (new Retype())->assert($this->type, $type, $this->nullCount());

        return new self($type, (new PhysicalFor())->type($type), $this->physicalValue, $this->count);
    }
}
