<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema\Diff;

use Flow\PostgreSql\Schema\{Column, Table, Trigger};
use Flow\PostgreSql\Schema\Constraint\PrimaryKey;

final readonly class TableComparator
{
    public function __construct(
        private IndexComparator $indexComparator,
        private ConstraintComparator $constraintComparator,
        private RenameStrategy $renameStrategy,
    ) {
    }

    public function compare(Table $source, Table $target) : TableDiff
    {
        $sourceColumnMap = [];

        foreach ($source->columns as $col) {
            $sourceColumnMap[$col->name] = $col;
        }

        $targetColumnMap = [];

        foreach ($target->columns as $col) {
            $targetColumnMap[$col->name] = $col;
        }

        $addedColumns = [];
        $removedColumns = [];
        $modifiedColumns = [];

        foreach ($targetColumnMap as $name => $col) {
            if (!array_key_exists($name, $sourceColumnMap)) {
                $addedColumns[$name] = $col;
            }
        }

        foreach ($sourceColumnMap as $name => $col) {
            if (!array_key_exists($name, $targetColumnMap)) {
                $removedColumns[$name] = $col;
            }
        }

        $this->detectColumnRenames($source->qualifiedName(), $addedColumns, $removedColumns, $modifiedColumns);

        foreach ($sourceColumnMap as $name => $sourceCol) {
            if (!array_key_exists($name, $targetColumnMap)) {
                continue;
            }

            if (!$sourceCol->isEqual($targetColumnMap[$name])) {
                $modifiedColumns[] = new ColumnDiff($source->qualifiedName(), $sourceCol, $targetColumnMap[$name]);
            }
        }

        $indexes = $this->indexComparator->compare($source->indexes, $target->indexes);
        $foreignKeys = $this->constraintComparator->diffForeignKeys($source->foreignKeys, $target->foreignKeys);
        $uniqueConstraints = $this->constraintComparator->diffUniqueConstraints($source->uniqueConstraints, $target->uniqueConstraints);
        $checkConstraints = $this->constraintComparator->diffCheckConstraints($source->checkConstraints, $target->checkConstraints);
        $excludeConstraints = $this->constraintComparator->diffExcludeConstraints($source->excludeConstraints, $target->excludeConstraints);
        $triggers = $this->diffTriggers($source->triggers, $target->triggers);

        $partitionChanged = $source->partitionStrategy !== $target->partitionStrategy
            || $source->partitionColumns !== $target->partitionColumns;

        $sourceInherits = $source->inherits;
        $targetInherits = $target->inherits;
        \sort($sourceInherits);
        \sort($targetInherits);
        $addedInherits = \array_values(\array_diff($targetInherits, $sourceInherits));
        $removedInherits = \array_values(\array_diff($sourceInherits, $targetInherits));

        $tablespaceChanged = $source->tablespace !== $target->tablespace;

        return new TableDiff(
            $source,
            $target,
            \array_values($addedColumns),
            \array_values($removedColumns),
            $modifiedColumns,
            $this->diffPrimaryKeyAdded($source->primaryKey, $target->primaryKey),
            $this->diffPrimaryKeyRemoved($source->primaryKey, $target->primaryKey),
            $indexes->added,
            $indexes->removed,
            $indexes->renamed ?? [],
            $foreignKeys->added,
            $foreignKeys->removed,
            $uniqueConstraints->added,
            $uniqueConstraints->removed,
            $checkConstraints->added,
            $checkConstraints->removed,
            $excludeConstraints->added,
            $excludeConstraints->removed,
            $triggers->added,
            $triggers->removed,
            $source->unlogged !== $target->unlogged,
            $partitionChanged,
            $addedInherits,
            $removedInherits,
            $tablespaceChanged,
        );
    }

    /**
     * @param array<string, Column> $addedColumns
     * @param array<string, Column> $removedColumns
     * @param list<ColumnDiff> $modifiedColumns
     */
    private function detectColumnRenames(string $qualifiedTableName, array &$addedColumns, array &$removedColumns, array &$modifiedColumns) : void
    {
        $candidates = [];

        foreach ($addedColumns as $addedName => $addedCol) {
            foreach ($removedColumns as $removedName => $removedCol) {
                if ($addedCol->isEqualStructure($removedCol)) {
                    $candidates[] = new RenameCandidate($addedName, $removedName);
                }
            }
        }

        foreach ($this->renameStrategy->resolve($candidates) as $match) {
            $modifiedColumns[] = new ColumnDiff($qualifiedTableName, $removedColumns[$match->removedName], $addedColumns[$match->addedName]);
            unset($addedColumns[$match->addedName], $removedColumns[$match->removedName]);
        }
    }

    private function diffPrimaryKeyAdded(?PrimaryKey $source, ?PrimaryKey $target) : ?PrimaryKey
    {
        if ($target === null) {
            return null;
        }

        if ($source === null) {
            return $target;
        }

        if ($source->isEqualStructure($target)) {
            return null;
        }

        return $target;
    }

    private function diffPrimaryKeyRemoved(?PrimaryKey $source, ?PrimaryKey $target) : ?PrimaryKey
    {
        if ($source === null) {
            return null;
        }

        if ($target === null) {
            return $source;
        }

        if ($source->isEqualStructure($target)) {
            return null;
        }

        return $source;
    }

    /**
     * @param list<Trigger> $sourceTriggers
     * @param list<Trigger> $targetTriggers
     *
     * @return ChangeSet<Trigger, mixed>
     */
    private function diffTriggers(array $sourceTriggers, array $targetTriggers) : ChangeSet
    {
        $sourceMap = [];

        foreach ($sourceTriggers as $trigger) {
            $sourceMap[$trigger->name] = $trigger;
        }

        $targetMap = [];

        foreach ($targetTriggers as $trigger) {
            $targetMap[$trigger->name] = $trigger;
        }

        $added = [];
        $removed = [];

        foreach ($targetMap as $name => $trigger) {
            if (!array_key_exists($name, $sourceMap)) {
                $added[] = $trigger;
            } elseif (!$sourceMap[$name]->isEqual($trigger)) {
                $removed[] = $sourceMap[$name];
                $added[] = $trigger;
            }
        }

        foreach ($sourceMap as $name => $trigger) {
            if (!array_key_exists($name, $targetMap)) {
                $removed[] = $trigger;
            }
        }

        return new ChangeSet($added, $removed);
    }
}
