<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema\Diff;

use Flow\PostgreSql\Schema\Column;
use Flow\PostgreSql\Schema\Constraint\PrimaryKey;
use Flow\PostgreSql\Schema\Table;

final readonly class TableStructureComparator
{
    public function __construct(
        private RenameStrategy $renameStrategy,
    ) {}

    /**
     * @param list<Table> $added
     * @param list<Table> $removed
     *
     * @return ChangeSet<Table, mixed>
     */
    public function detectTableRenames(array $added, array $removed): ChangeSet
    {
        $addedMap = [];

        foreach ($added as $table) {
            $addedMap[$table->name] = $table;
        }

        $removedMap = [];

        foreach ($removed as $table) {
            $removedMap[$table->name] = $table;
        }

        $candidates = [];

        foreach ($addedMap as $addedName => $addedTable) {
            foreach ($removedMap as $removedName => $removedTable) {
                if ($this->haveEqualStructure($addedTable, $removedTable)) {
                    $candidates[] = new RenameCandidate($addedName, $removedName);
                }
            }
        }

        $renamed = [];

        foreach ($this->renameStrategy->resolve($candidates) as $match) {
            $renamed[$removedMap[$match->removedName]->qualifiedName()] = $addedMap[$match->addedName];
            unset($addedMap[$match->addedName], $removedMap[$match->removedName]);
        }

        return new ChangeSet(
            \array_values($addedMap),
            \array_values($removedMap),
            renamed: $renamed !== [] ? $renamed : null,
        );
    }

    public function haveEqualStructure(Table $a, Table $b): bool
    {
        if ($a->schema !== $b->schema) {
            return false;
        }

        if (
            $a->unlogged !== $b->unlogged
            || $a->partitionStrategy !== $b->partitionStrategy
            || $a->tablespace !== $b->tablespace
        ) {
            return false;
        }

        $aPartCols = $a->partitionColumns;
        $bPartCols = $b->partitionColumns;
        \sort($aPartCols);
        \sort($bPartCols);

        if ($aPartCols !== $bPartCols) {
            return false;
        }

        $aInherits = $a->inherits;
        $bInherits = $b->inherits;
        \sort($aInherits);
        \sort($bInherits);

        if ($aInherits !== $bInherits) {
            return false;
        }

        if (!$this->columnListsEqual($a->columns, $b->columns)) {
            return false;
        }

        if (!$this->primaryKeysStructurallyEqual($a->primaryKey, $b->primaryKey)) {
            return false;
        }

        return $this->listsStructurallyEqual($a->indexes, $b->indexes, static fn($x, $y) => $x->isEqualStructure(
            $y,
        )) && $this->listsStructurallyEqual($a->foreignKeys, $b->foreignKeys, static fn($x, $y) => $x->isEqualStructure(
            $y,
        )) && $this->listsStructurallyEqual($a->uniqueConstraints, $b->uniqueConstraints, static fn(
            $x,
            $y,
        ) => $x->isEqualStructure(
            $y,
        )) && $this->listsStructurallyEqual($a->checkConstraints, $b->checkConstraints, static fn(
            $x,
            $y,
        ) => $x->isEqualStructure(
            $y,
        )) && $this->listsStructurallyEqual($a->excludeConstraints, $b->excludeConstraints, static fn(
            $x,
            $y,
        ) => $x->isEqualStructure($y)) && $this->listsStructurallyEqual($a->triggers, $b->triggers, static fn(
            $x,
            $y,
        ) => $x->isEqualStructure($y));
    }

    /**
     * @param list<Column> $a
     * @param list<Column> $b
     */
    private function columnListsEqual(array $a, array $b): bool
    {
        if (\count($a) !== \count($b)) {
            return false;
        }

        $mapA = [];

        foreach ($a as $col) {
            $mapA[$col->name] = $col;
        }

        $mapB = [];

        foreach ($b as $col) {
            $mapB[$col->name] = $col;
        }

        if (\count($mapA) !== \count($mapB)) {
            return false;
        }

        foreach ($mapA as $name => $colA) {
            if (!array_key_exists($name, $mapB)) {
                return false;
            }

            if (!$colA->isEqualStructure($mapB[$name])) {
                return false;
            }
        }

        return true;
    }

    /**
     * @template T
     *
     * @param list<T> $a
     * @param list<T> $b
     * @param callable(T, T): bool $equalsFn
     */
    private function listsStructurallyEqual(array $a, array $b, callable $equalsFn): bool
    {
        if (\count($a) !== \count($b)) {
            return false;
        }

        $matched = [];

        foreach ($a as $itemA) {
            foreach ($b as $j => $itemB) {
                if (!array_key_exists($j, $matched) && $equalsFn($itemA, $itemB)) {
                    $matched[$j] = true;

                    continue 2;
                }
            }

            return false;
        }

        return true;
    }

    private function primaryKeysStructurallyEqual(?PrimaryKey $a, ?PrimaryKey $b): bool
    {
        if ($a === null && $b === null) {
            return true;
        }

        if ($a === null || $b === null) {
            return false;
        }

        return $a->isEqualStructure($b);
    }
}
