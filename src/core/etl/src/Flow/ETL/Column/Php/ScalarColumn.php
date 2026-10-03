<?php

declare(strict_types=1);

namespace Flow\ETL\Column\Php;

use Flow\ETL\Column\Column;
use Flow\ETL\Column\Layout\LayoutFor;
use Flow\ETL\Column\Layout\NullCount;
use Flow\ETL\Column\Layout\Validity;
use Flow\ETL\Column\Physical\Physical;
use Flow\ETL\Column\Physical\PhysicalFor;
use Flow\ETL\Column\Retype;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\Types\Type;

use function array_merge;
use function array_slice;
use function count;
use function sprintf;

final readonly class ScalarColumn implements Column
{
    /**
     * @param Type<mixed> $type
     * @param list<mixed> $values physicals, null for a null cell
     */
    public function __construct(
        public Type $type,
        public Physical $physical,
        public array $values,
        public int $nullCount,
    ) {}

    public function at(int $i): mixed
    {
        return $this->values[$i];
    }

    public function concat(Column ...$others): Column
    {
        $values = [$this->values];
        $nullCount = $this->nullCount;

        foreach ($others as $other) {
            if (!$other instanceof self) {
                throw new InvalidArgumentException(sprintf('%s cannot concat %s', self::class, $other::class));
            }

            $values[] = $other->values;
            $nullCount += $other->nullCount;
        }

        return new self($this->type, $this->physical, array_merge(...$values), $nullCount);
    }

    public function count(): int
    {
        return count($this->values);
    }

    public function encode(): array
    {
        return [
            (new Validity())->fromValues($this->values),
            ...(new LayoutFor())
                ->type($this->type)
                ->encode($this->values),
        ];
    }

    public function isNull(int $i): bool
    {
        return $this->values[$i] === null;
    }

    public function nullCount(): int
    {
        return $this->nullCount;
    }

    public function physicals(): array
    {
        return $this->values;
    }

    public function slice(int $offset, int $length): Column
    {
        $values = array_slice($this->values, $offset, $length);

        return new self(
            $this->type,
            $this->physical,
            $values,
            $this->nullCount === 0 ? 0 : (new NullCount())->of($values),
        );
    }

    public function take(array $indices): Column
    {
        $values = [];

        foreach ($indices as $index) {
            $values[] = $this->values[$index];
        }

        return new self(
            $this->type,
            $this->physical,
            $values,
            $this->nullCount === 0 ? 0 : (new NullCount())->of($values),
        );
    }

    public function type(): Type
    {
        return $this->type;
    }

    public function value(int $i): mixed
    {
        return $this->values[$i] === null ? null : $this->physical->fromPhysical($this->values[$i]);
    }

    public function values(): array
    {
        return $this->physical->fromPhysicalAll($this->values);
    }

    public function withType(Type $type): Column
    {
        (new Retype())->assert($this->type, $type, $this->nullCount);

        return new self($type, (new PhysicalFor())->type($type), $this->values, $this->nullCount);
    }
}
