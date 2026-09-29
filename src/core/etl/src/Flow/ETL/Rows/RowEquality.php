<?php

declare(strict_types=1);

namespace Flow\ETL\Rows;

use Flow\ETL\Column\ComparableValues;
use Flow\ETL\Row\TypedValueComparator;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\Types\Type;

use function array_fill;
use function array_key_exists;
use function array_keys;

final readonly class RowEquality
{
    public function __construct(
        private TypedValueComparator $comparator = new TypedValueComparator(),
    ) {}

    /**
     * Rows that can be equal share a key: the RowHashes of the columns whose hash follows their equality (numbers,
     * strings, bools, dates, times). Other columns (json, lists, ...) compare equal across different hashes.
     *
     * @return list<string>
     */
    public function bucketKeys(Rows $rows, Schema $schema): array
    {
        $comparable = new ComparableValues();
        $exact = [];

        foreach ($schema->definitions() as $name => $definition) {
            if (
                $comparable->orderedByPhysical($definition->type())
                && $rows->schema()->findDefinition($name) !== null
            ) {
                $exact[] = $name;
            }
        }

        if ($exact === [] || $rows->isEmpty()) {
            // @mago-ignore analysis:possibly-invalid-argument
            return $rows->isEmpty() ? [] : array_fill(0, $rows->count(), '');
        }

        return (new RowHashes())->of($rows->project($rows->schema()->keep(...$exact)));
    }

    /**
     * @return array<array-key, list<mixed>>
     */
    public function columns(Rows $rows): array
    {
        $values = [];

        foreach ($rows->columns() as $name => $column) {
            $values[$name] = $column->values();
        }

        return $values;
    }

    /**
     * @param array<array-key, Type<mixed>> $types
     * @param array<array-key, list<mixed>> $left
     * @param array<array-key, list<mixed>> $right
     */
    public function equal(array $types, array $left, int $i, array $right, int $j): bool
    {
        foreach ($types as $name => $type) {
            if (!array_key_exists($name, $left) || !array_key_exists($name, $right)) {
                continue;
            }

            if (!$this->comparator->equals($type, $left[$name][$i], $right[$name][$j])) {
                return false;
            }
        }

        return true;
    }

    /**
     * @return list<int> rows of $rows with no equal row in $other, compared by $schema's types
     */
    public function notIn(Rows $rows, Rows $other, Schema $schema): array
    {
        $values = $this->columns($rows);
        $others = $this->columns($other);

        if (array_keys($values) !== array_keys($others)) {
            // @mago-ignore analysis:possibly-invalid-argument
            return $rows->isEmpty() ? [] : array_keys(array_fill(0, $rows->count(), true));
        }

        $types = $this->types($schema);

        /** @var array<string, list<int>> $buckets */
        $buckets = [];

        foreach ($this->bucketKeys($other, $schema) as $j => $key) {
            $buckets[$key][] = $j;
        }

        $kept = [];

        foreach ($this->bucketKeys($rows, $schema) as $i => $key) {
            foreach ($buckets[$key] ?? [] as $j) {
                if ($this->equal($types, $values, $i, $others, $j)) {
                    continue 2;
                }
            }

            $kept[] = $i;
        }

        return $kept;
    }

    /**
     * @return array<array-key, Type<mixed>>
     */
    public function types(Schema $schema): array
    {
        $types = [];

        foreach ($schema->definitions() as $name => $definition) {
            $types[$name] = $definition->type();
        }

        return $types;
    }

    /**
     * @return list<int> the first row of every group of equal rows
     */
    public function unique(Rows $rows): array
    {
        $values = $this->columns($rows);
        $types = $this->types($rows->schema());

        /** @var array<string, list<int>> $buckets */
        $buckets = [];
        $kept = [];

        foreach ($this->bucketKeys($rows, $rows->schema()) as $i => $key) {
            foreach ($buckets[$key] ?? [] as $j) {
                if ($this->equal($types, $values, $i, $values, $j)) {
                    continue 2;
                }
            }

            $buckets[$key][] = $i;
            $kept[] = $i;
        }

        return $kept;
    }
}
