<?php

declare(strict_types=1);

namespace Flow\ETL\Column\Php;

use Flow\ETL\Column\Column;
use Flow\Types\Type;

use function array_key_exists;
use function assert;
use function is_array;

final class StructColumnBuilder implements PhpColumnBuilder
{
    private int $count = 0;

    /**
     * @var array<int, true>
     */
    private array $nulls = [];

    /**
     * @param Type<mixed> $type
     * @param non-empty-array<array-key, PhpColumnBuilder> $children keyed by element name, in element order
     */
    public function __construct(
        private readonly Type $type,
        private readonly array $children,
    ) {}

    public function appendPhysical(mixed $physical): void
    {
        if ($physical === null) {
            $this->nulls[$this->count++] = true;

            foreach ($this->children as $child) {
                $child->appendPhysical(null);
            }

            return;
        }

        assert(is_array($physical));
        $this->count++;

        foreach ($this->children as $name => $child) {
            $child->appendPhysical(array_key_exists($name, $physical) ? $physical[$name] : null);
        }
    }

    public function appendPhysicals(array $physicals, ?int $nullCount = null): void
    {
        $names = array_keys($this->children);
        $columns = array_fill_keys($names, []);

        // @mago-ignore analysis:mixed-assignment
        foreach ($physicals as $physical) {
            if ($physical === null) {
                $this->nulls[$this->count] = true;
            }

            $this->count++;

            foreach ($names as $name) {
                $columns[$name][] = $physical[$name] ?? null;
            }
        }

        foreach ($this->children as $name => $child) {
            $child->appendPhysicals($columns[$name]);
        }
    }

    public function count(): int
    {
        return $this->count;
    }

    public function finish(): Column
    {
        $children = [];

        foreach ($this->children as $name => $child) {
            $children[$name] = $child->finish();
        }

        return new StructColumn($this->type, $children, $this->nulls);
    }
}
