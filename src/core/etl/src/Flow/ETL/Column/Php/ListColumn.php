<?php

declare(strict_types=1);

namespace Flow\ETL\Column\Php;

use Flow\ETL\Column\Column;
use Flow\ETL\Column\Layout\Offsets;
use Flow\ETL\Column\Layout\Validity;
use Flow\ETL\Column\Retype;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\Types\Type;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\OptionalType;

use function array_key_exists;
use function array_slice;
use function assert;
use function count;
use function range;
use function sprintf;

final readonly class ListColumn implements Column
{
    /**
     * @param Type<mixed> $type a ListType, or an OptionalType over one for a child column
     * @param list<int> $offsets count() + 1 offsets, from 0
     * @param array<int, true> $nulls
     */
    public function __construct(
        public Type $type,
        public array $offsets,
        public Column $element,
        public array $nulls,
    ) {}

    public function at(int $i): mixed
    {
        if (array_key_exists($i, $this->nulls)) {
            return null;
        }

        return $this->element->slice($this->offsets[$i], $this->offsets[$i + 1] - $this->offsets[$i])->physicals();
    }

    public function concat(Column ...$others): Column
    {
        $offsets = $this->offsets;
        $nulls = $this->nulls;
        $rows = $this->count();
        $elements = [];

        foreach ($others as $other) {
            if (!$other instanceof self) {
                throw new InvalidArgumentException(sprintf('%s cannot concat %s', self::class, $other::class));
            }

            $base = $offsets[count($offsets) - 1];

            for ($k = 1, $last = count($other->offsets); $k < $last; $k++) {
                $offsets[] = $base + $other->offsets[$k];
            }

            foreach ($other->nulls as $index => $_) {
                $nulls[$rows + $index] = true;
            }

            $rows += $other->count();
            $elements[] = $other->element;
        }

        return new self($this->type, $offsets, $this->element->concat(...$elements), $nulls);
    }

    public function count(): int
    {
        return count($this->offsets) - 1;
    }

    public function encode(): array
    {
        return [
            (new Validity())->fromNulls($this->nulls, $this->count()),
            (new Offsets())->pack($this->offsets),
            ...$this->element->encode(),
        ];
    }

    public function isNull(int $i): bool
    {
        return array_key_exists($i, $this->nulls);
    }

    public function nullCount(): int
    {
        return count($this->nulls);
    }

    public function physicals(): array
    {
        $elements = $this->element->physicals();
        $physicals = [];

        for ($i = 0, $count = $this->count(); $i < $count; $i++) {
            $physicals[] = array_key_exists($i, $this->nulls)
                ? null
                : array_slice($elements, $this->offsets[$i], $this->offsets[$i + 1] - $this->offsets[$i]);
        }

        return $physicals;
    }

    public function slice(int $offset, int $length): Column
    {
        return $this->take($length === 0 ? [] : range($offset, $offset + $length - 1));
    }

    public function take(array $indices): Column
    {
        $offsets = [0];
        $nulls = [];
        $elementIndices = [];

        foreach ($indices as $position => $index) {
            if (array_key_exists($index, $this->nulls)) {
                $nulls[$position] = true;
            }

            for ($j = $this->offsets[$index], $end = $this->offsets[$index + 1]; $j < $end; $j++) {
                $elementIndices[] = $j;
            }

            $offsets[] = count($elementIndices);
        }

        return new self($this->type, $offsets, $this->element->take($elementIndices), $nulls);
    }

    public function type(): Type
    {
        return $this->type;
    }

    public function value(int $i): mixed
    {
        if (array_key_exists($i, $this->nulls)) {
            return null;
        }

        return $this->element->slice($this->offsets[$i], $this->offsets[$i + 1] - $this->offsets[$i])->values();
    }

    public function values(): array
    {
        $elements = $this->element->values();
        $values = [];

        for ($i = 0, $count = $this->count(); $i < $count; $i++) {
            $values[] = array_key_exists($i, $this->nulls)
                ? null
                : array_slice($elements, $this->offsets[$i], $this->offsets[$i + 1] - $this->offsets[$i]);
        }

        return $values;
    }

    public function withType(Type $type): Column
    {
        (new Retype())->assert($this->type, $type, count($this->nulls));

        $list = $type instanceof OptionalType ? $type->base() : $type;
        assert($list instanceof ListType);

        return new self($type, $this->offsets, $this->element->withType($list->element()), $this->nulls);
    }
}
