<?php

declare(strict_types=1);

namespace Flow\ETL\Column\Php;

use Flow\ETL\Column\Column;
use Flow\ETL\Column\Layout\Validity;
use Flow\ETL\Column\Retype;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\Types\Type;
use Flow\Types\Type\Logical\OptionalType;
use Flow\Types\Type\Logical\StructureType;

use function array_key_exists;
use function array_key_first;
use function assert;
use function count;
use function range;
use function sprintf;

final readonly class StructColumn implements Column
{
    /**
     * @param Type<mixed> $type a StructureType, or an OptionalType over one for a child column
     * @param non-empty-array<array-key, Column> $children keyed by element name, in element order
     * @param array<int, true> $nulls
     */
    public function __construct(
        public Type $type,
        public array $children,
        public array $nulls,
    ) {}

    public function at(int $i): mixed
    {
        if (array_key_exists($i, $this->nulls)) {
            return null;
        }

        $physical = [];

        foreach ($this->structure()->elements() as $element) {
            $child = $this->children[$element->name];

            if ($child->isNull($i)) {
                if (!$element->optional) {
                    $physical[$element->name] = null;
                }

                continue;
            }

            $physical[$element->name] = $child->at($i);
        }

        return $physical;
    }

    public function concat(Column ...$others): Column
    {
        $nulls = $this->nulls;
        $rows = $this->count();
        $children = [];

        foreach ($others as $other) {
            if (!$other instanceof self) {
                throw new InvalidArgumentException(sprintf('%s cannot concat %s', self::class, $other::class));
            }

            foreach ($other->nulls as $index => $_) {
                $nulls[$rows + $index] = true;
            }

            $rows += $other->count();

            foreach ($other->children as $name => $child) {
                $children[$name][] = $child;
            }
        }

        $concatenated = [];

        foreach ($this->children as $name => $child) {
            $concatenated[$name] = $child->concat(...$children[$name] ?? []);
        }

        return new self($this->type, $concatenated, $nulls);
    }

    public function count(): int
    {
        return $this->children[array_key_first($this->children)]->count();
    }

    public function encode(): array
    {
        $buffers = [(new Validity())->fromNulls($this->nulls, $this->count())];

        foreach ($this->children as $child) {
            foreach ($child->encode() as $buffer) {
                $buffers[] = $buffer;
            }
        }

        return $buffers;
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
        $elements = $this->structure()->elements();
        $children = [];

        foreach ($elements as $element) {
            $children[$element->name] = $this->children[$element->name]->physicals();
        }

        $physicals = [];

        for ($i = 0, $count = $this->count(); $i < $count; $i++) {
            if (array_key_exists($i, $this->nulls)) {
                $physicals[] = null;

                continue;
            }

            $physical = [];

            foreach ($elements as $element) {
                // @mago-ignore analysis:mixed-assignment
                $cell = $children[$element->name][$i];

                if ($cell === null && $element->optional) {
                    continue;
                }

                $physical[$element->name] = $cell;
            }

            $physicals[] = $physical;
        }

        return $physicals;
    }

    public function slice(int $offset, int $length): Column
    {
        return $this->take($length === 0 ? [] : range($offset, $offset + $length - 1));
    }

    public function structure(): StructureType
    {
        $structure = $this->type instanceof OptionalType ? $this->type->base() : $this->type;
        assert($structure instanceof StructureType);

        return $structure;
    }

    public function take(array $indices): Column
    {
        $nulls = [];

        foreach ($indices as $position => $index) {
            if (array_key_exists($index, $this->nulls)) {
                $nulls[$position] = true;
            }
        }

        $children = [];

        foreach ($this->children as $name => $child) {
            $children[$name] = $child->take($indices);
        }

        return new self($this->type, $children, $nulls);
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

        $value = [];

        foreach ($this->structure()->elements() as $element) {
            $child = $this->children[$element->name];

            if ($child->isNull($i)) {
                if (!$element->optional) {
                    $value[$element->name] = null;
                }

                continue;
            }

            $value[$element->name] = $child->value($i);
        }

        return $value;
    }

    public function values(): array
    {
        $elements = $this->structure()->elements();
        $children = [];

        foreach ($elements as $element) {
            $children[$element->name] = $this->children[$element->name]->values();
        }

        $values = [];

        for ($i = 0, $count = $this->count(); $i < $count; $i++) {
            if (array_key_exists($i, $this->nulls)) {
                $values[] = null;

                continue;
            }

            $value = [];

            foreach ($elements as $element) {
                // @mago-ignore analysis:mixed-assignment
                $cell = $children[$element->name][$i];

                if ($cell === null && $element->optional) {
                    continue;
                }

                $value[$element->name] = $cell;
            }

            $values[] = $value;
        }

        return $values;
    }

    public function withType(Type $type): Column
    {
        (new Retype())->assert($this->type, $type, count($this->nulls));

        $structure = $type instanceof OptionalType ? $type->base() : $type;
        assert($structure instanceof StructureType);

        $from = $this->structure()->elements();
        $children = [];

        foreach ($structure->elements() as $position => $element) {
            $child = $this->children[$element->name];

            if ($from[$position]->optional && !$element->optional) {
                $absent = 0;

                for ($i = 0, $count = $child->count(); $i < $count; $i++) {
                    if (!array_key_exists($i, $this->nulls) && $child->isNull($i)) {
                        $absent++;
                    }
                }

                if ($absent > 0) {
                    throw new InvalidArgumentException(sprintf(
                        '%d absent values under the required element "%s"',
                        $absent,
                        $element->name,
                    ));
                }
            }

            $children[$element->name] = $child->withType($element->type);
        }

        return new self($type, $children, $this->nulls);
    }
}
