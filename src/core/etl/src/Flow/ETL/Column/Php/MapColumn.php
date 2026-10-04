<?php

declare(strict_types=1);

namespace Flow\ETL\Column\Php;

use Flow\ETL\Column\Column;
use Flow\ETL\Column\Layout\Offsets;
use Flow\ETL\Column\Layout\Validity;
use Flow\ETL\Column\Retype;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\Types\Type;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\OptionalType;

use function array_combine;
use function array_key_exists;
use function array_slice;
use function assert;
use function count;
use function range;
use function sprintf;

final readonly class MapColumn implements Column
{
    /**
     * @param Type<mixed> $type a MapType, or an OptionalType over one for a child column
     * @param list<int> $offsets count() + 1 offsets, from 0
     * @param array<int, true> $nulls
     */
    public function __construct(
        public Type $type,
        public array $offsets,
        public Column $keys,
        public Column $values,
        public array $nulls,
    ) {}

    public function at(int $i): mixed
    {
        if (array_key_exists($i, $this->nulls)) {
            return null;
        }

        $length = $this->offsets[$i + 1] - $this->offsets[$i];

        /** @var list<int|string> $keys */
        $keys = $this->keys->slice($this->offsets[$i], $length)->physicals();

        return array_combine($keys, $this->values->slice($this->offsets[$i], $length)->physicals());
    }

    public function concat(Column ...$others): Column
    {
        $offsets = $this->offsets;
        $nulls = $this->nulls;
        $rows = $this->count();
        $keys = [];
        $values = [];

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
            $keys[] = $other->keys;
            $values[] = $other->values;
        }

        return new self(
            $this->type,
            $offsets,
            $this->keys->concat(...$keys),
            $this->values->concat(...$values),
            $nulls,
        );
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
            '',
            ...$this->keys->encode(),
            ...$this->values->encode(),
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
        $keys = $this->keys->physicals();
        $values = $this->values->physicals();
        $physicals = [];

        for ($i = 0, $count = $this->count(); $i < $count; $i++) {
            if (array_key_exists($i, $this->nulls)) {
                $physicals[] = null;

                continue;
            }

            $length = $this->offsets[$i + 1] - $this->offsets[$i];

            /** @var list<int|string> $rowKeys */
            $rowKeys = array_slice($keys, $this->offsets[$i], $length);
            $physicals[] = array_combine($rowKeys, array_slice($values, $this->offsets[$i], $length));
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
        $entryIndices = [];

        foreach ($indices as $position => $index) {
            if (array_key_exists($index, $this->nulls)) {
                $nulls[$position] = true;
            }

            for ($j = $this->offsets[$index], $end = $this->offsets[$index + 1]; $j < $end; $j++) {
                $entryIndices[] = $j;
            }

            $offsets[] = count($entryIndices);
        }

        return new self(
            $this->type,
            $offsets,
            $this->keys->take($entryIndices),
            $this->values->take($entryIndices),
            $nulls,
        );
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

        $length = $this->offsets[$i + 1] - $this->offsets[$i];

        /** @var list<int|string> $keys */
        $keys = $this->keys->slice($this->offsets[$i], $length)->values();

        return array_combine($keys, $this->values->slice($this->offsets[$i], $length)->values());
    }

    public function values(): array
    {
        $keys = $this->keys->values();
        $values = $this->values->values();
        $result = [];

        for ($i = 0, $count = $this->count(); $i < $count; $i++) {
            if (array_key_exists($i, $this->nulls)) {
                $result[] = null;

                continue;
            }

            $length = $this->offsets[$i + 1] - $this->offsets[$i];

            /** @var list<int|string> $rowKeys */
            $rowKeys = array_slice($keys, $this->offsets[$i], $length);
            $result[] = array_combine($rowKeys, array_slice($values, $this->offsets[$i], $length));
        }

        return $result;
    }

    public function withType(Type $type): Column
    {
        (new Retype())->assert($this->type, $type, count($this->nulls));

        $map = $type instanceof OptionalType ? $type->base() : $type;
        assert($map instanceof MapType);

        return new self(
            $type,
            $this->offsets,
            $this->keys->withType($map->key()),
            $this->values->withType($map->value()),
            $this->nulls,
        );
    }
}
