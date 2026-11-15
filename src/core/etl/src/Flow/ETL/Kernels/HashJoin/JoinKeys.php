<?php

declare(strict_types=1);

namespace Flow\ETL\Kernels\HashJoin;

use Flow\ETL\Values\Column\ComparableValues;
use Flow\ETL\Values\Column\ValueEquality;
use Flow\ETL\Values\Grouping\KeyEncoder;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\ETL\Values\Schema\Definitions;
use Flow\Types\Type;

use function array_key_exists;

final readonly class JoinKeys
{
    /**
     * @param list<string> $left
     * @param list<string> $right column names, pairwise with $left
     * @param bool $exact the condition is only Equal comparisons, so rows whose keys are equal and not null meet it
     */
    public function __construct(
        public array $left,
        public array $right,
        public bool $exact = false,
    ) {}

    /**
     * @param array<int, Type<mixed>> $types
     *
     * @return Rows the left key columns of $rows, each cast to its type in $types, named "0", "1", … by position
     */
    public function leftKeys(Rows $rows, array $types): Rows
    {
        return $this->keysOf($this->left, $rows, $types);
    }

    /**
     * @param array<int, Type<mixed>> $types
     *
     * @return Rows the right key columns of $rows, each cast to its type in $types, named "0", "1", … by position
     */
    public function rightKeys(Rows $rows, array $types): Rows
    {
        return $this->keysOf($this->right, $rows, $types);
    }

    /**
     * The keys alone decide the condition for $keys: exact, no null, every column type compared by physical.
     */
    public function decides(Rows $keys): bool
    {
        if (!$this->exact) {
            return false;
        }

        $comparable = new ComparableValues();

        foreach ($keys->columns() as $column) {
            if ($column->nullCount() !== 0 || !$comparable->equalByPhysical($column->type())) {
                return false;
            }
        }

        return true;
    }

    /**
     * One key encoder per side, both keying in types().
     *
     * @return array{KeyEncoder, KeyEncoder}
     */
    public function encoders(?Schema $left, ?Schema $right): array
    {
        $types = $this->types($left, $right);

        return [new KeyEncoder($this->left, $types), new KeyEncoder($this->right, $types)];
    }

    /**
     * By position, the one type each key pair compares in, so a key of either side meets its equal on the other;
     * a pair without both schemas keys in its own types.
     *
     * @return array<int, Type<mixed>>
     */
    public function types(?Schema $left, ?Schema $right): array
    {
        $types = [];

        foreach ($this->left as $position => $name) {
            $leftDefinition = $left?->findDefinition($name);
            $rightDefinition = $right?->findDefinition($this->right[$position]);

            if ($leftDefinition !== null && $rightDefinition !== null) {
                $types[$position] = (new ValueEquality())->type($leftDefinition->type(), $rightDefinition->type());
            }
        }

        return $types;
    }

    /**
     * @param list<string> $names
     * @param array<int, Type<mixed>> $types
     */
    private function keysOf(array $names, Rows $rows, array $types): Rows
    {
        $equality = new ValueEquality();
        $definitions = [];
        $columns = [];

        foreach ($names as $position => $name) {
            $column = array_key_exists($position, $types)
                ? $equality->cast($rows->column($name), $types[$position], $name)
                : $rows->column($name);
            $definitions[] = Definitions::of((string) $position, $column->type(), $column->nullCount() > 0);
            $columns[$position] = $column;
        }

        return Rows::fromColumns(new Schema(...$definitions), $columns, $rows->count());
    }
}
