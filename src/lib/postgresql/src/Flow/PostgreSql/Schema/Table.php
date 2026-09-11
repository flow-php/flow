<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema;

use Flow\PostgreSql\Parser\ExpressionParser;
use Flow\PostgreSql\QueryBuilder\Condition\ConditionFactory;
use Flow\PostgreSql\QueryBuilder\Expression\ExpressionFactory;
use Flow\PostgreSql\QueryBuilder\Schema\Constraint\CheckConstraint as CheckConstraintBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Constraint\ForeignKeyConstraint;
use Flow\PostgreSql\QueryBuilder\Schema\Constraint\PrimaryKeyConstraint;
use Flow\PostgreSql\QueryBuilder\Schema\Constraint\UniqueConstraint as UniqueConstraintBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Index\IndexMethod as QbIndexMethod;
use Flow\PostgreSql\QueryBuilder\Schema\Trigger\TriggerEvent as QbTriggerEvent;
use Flow\PostgreSql\QueryBuilder\Sql;
use Flow\PostgreSql\Schema\Constraint\CheckConstraint;
use Flow\PostgreSql\Schema\Constraint\ExcludeConstraint;
use Flow\PostgreSql\Schema\Constraint\ForeignKey;
use Flow\PostgreSql\Schema\Constraint\PrimaryKey;
use Flow\PostgreSql\Schema\Constraint\UniqueConstraint;
use Flow\PostgreSql\Schema\Exception\ColumnNotFoundException;

use function array_map;
use function array_values;
use function Flow\PostgreSql\DSL\column;
use function Flow\PostgreSql\DSL\create;

/**
 * @import-type ColumnShape from Column
 * @import-type IndexShape from Index
 * @import-type TriggerShape from Trigger
 * @import-type PrimaryKeyShape from PrimaryKey
 * @import-type ForeignKeyShape from ForeignKey
 * @import-type UniqueConstraintShape from UniqueConstraint
 * @import-type CheckConstraintShape from CheckConstraint
 * @import-type ExcludeConstraintShape from ExcludeConstraint
 *
 * @type TableShape = array{schema?: string, name: string, columns: non-empty-list<ColumnShape>, primary_key?: ?PrimaryKeyShape, indexes?: list<IndexShape>, foreign_keys?: list<ForeignKeyShape>, unique_constraints?: list<UniqueConstraintShape>, check_constraints?: list<CheckConstraintShape>, exclude_constraints?: list<ExcludeConstraintShape>, triggers?: list<TriggerShape>, unlogged?: bool, partition_strategy?: ?string, partition_columns?: list<string>, inherits?: list<string>, tablespace?: ?string}
 */
final readonly class Table
{
    /**
     * @param non-empty-list<Column> $columns
     * @param list<Index> $indexes
     * @param list<ForeignKey> $foreignKeys
     * @param list<UniqueConstraint> $uniqueConstraints
     * @param list<CheckConstraint> $checkConstraints
     * @param list<ExcludeConstraint> $excludeConstraints
     * @param list<Trigger> $triggers
     * @param list<string> $partitionColumns
     * @param list<string> $inherits
     */
    public function __construct(
        public string $schema,
        public string $name,
        public array $columns,
        public ?PrimaryKey $primaryKey = null,
        public array $indexes = [],
        public array $foreignKeys = [],
        public array $uniqueConstraints = [],
        public array $checkConstraints = [],
        public array $excludeConstraints = [],
        public array $triggers = [],
        public bool $unlogged = false,
        public ?PartitionStrategy $partitionStrategy = null,
        public array $partitionColumns = [],
        public array $inherits = [],
        public ?string $tablespace = null,
    ) {}

    /**
     * @param TableShape $data
     */
    public static function fromArray(array $data): self
    {
        return new self(
            schema: $data['schema'] ?? 'public',
            name: $data['name'],
            columns: array_map(static fn(array $col): Column => Column::fromArray($col), $data['columns']),
            primaryKey: array_key_exists('primary_key', $data) && $data['primary_key'] !== null
                ? PrimaryKey::fromArray($data['primary_key'])
                : null,
            indexes: array_map(static fn(array $idx): Index => Index::fromArray($idx), $data['indexes'] ?? []),
            foreignKeys: array_map(static fn(array $fk): ForeignKey => ForeignKey::fromArray(
                $fk,
            ), $data['foreign_keys'] ?? []),
            uniqueConstraints: array_map(static fn(array $uc): UniqueConstraint => UniqueConstraint::fromArray(
                $uc,
            ), $data['unique_constraints'] ?? []),
            checkConstraints: array_map(static fn(array $cc): CheckConstraint => CheckConstraint::fromArray(
                $cc,
            ), $data['check_constraints'] ?? []),
            excludeConstraints: array_map(static fn(array $ec): ExcludeConstraint => ExcludeConstraint::fromArray(
                $ec,
            ), $data['exclude_constraints'] ?? []),
            triggers: array_map(static fn(array $t): Trigger => Trigger::fromArray($t), $data['triggers'] ?? []),
            unlogged: $data['unlogged'] ?? false,
            partitionStrategy: array_key_exists('partition_strategy', $data) && $data['partition_strategy'] !== null
                ? PartitionStrategy::from($data['partition_strategy'])
                : null,
            partitionColumns: $data['partition_columns'] ?? [],
            inherits: $data['inherits'] ?? [],
            tablespace: $data['tablespace'] ?? null,
        );
    }

    public function column(string $name): Column
    {
        foreach ($this->columns as $column) {
            if ($column->name === $name) {
                return $column;
            }
        }

        throw ColumnNotFoundException::inTable($name, $this->name);
    }

    /**
     * @return list<string>
     */
    public function columnNames(): array
    {
        return array_map(static fn(Column $c): string => $c->name, $this->columns);
    }

    public function hasColumn(string $name): bool
    {
        foreach ($this->columns as $column) {
            if ($column->name === $name) {
                return true;
            }
        }

        return false;
    }

    public function withCheckConstraint(CheckConstraint $checkConstraint): self
    {
        return new self(
            schema: $this->schema,
            name: $this->name,
            columns: $this->columns,
            primaryKey: $this->primaryKey,
            indexes: $this->indexes,
            foreignKeys: $this->foreignKeys,
            uniqueConstraints: $this->uniqueConstraints,
            checkConstraints: [...$this->checkConstraints, $checkConstraint],
            excludeConstraints: $this->excludeConstraints,
            triggers: $this->triggers,
            unlogged: $this->unlogged,
            partitionStrategy: $this->partitionStrategy,
            partitionColumns: $this->partitionColumns,
            inherits: $this->inherits,
            tablespace: $this->tablespace,
        );
    }

    public function withExcludeConstraint(ExcludeConstraint $excludeConstraint): self
    {
        return new self(
            schema: $this->schema,
            name: $this->name,
            columns: $this->columns,
            primaryKey: $this->primaryKey,
            indexes: $this->indexes,
            foreignKeys: $this->foreignKeys,
            uniqueConstraints: $this->uniqueConstraints,
            checkConstraints: $this->checkConstraints,
            excludeConstraints: [...$this->excludeConstraints, $excludeConstraint],
            triggers: $this->triggers,
            unlogged: $this->unlogged,
            partitionStrategy: $this->partitionStrategy,
            partitionColumns: $this->partitionColumns,
            inherits: $this->inherits,
            tablespace: $this->tablespace,
        );
    }

    public function withForeignKey(ForeignKey $foreignKey): self
    {
        return new self(
            schema: $this->schema,
            name: $this->name,
            columns: $this->columns,
            primaryKey: $this->primaryKey,
            indexes: $this->indexes,
            foreignKeys: [...$this->foreignKeys, $foreignKey],
            uniqueConstraints: $this->uniqueConstraints,
            checkConstraints: $this->checkConstraints,
            excludeConstraints: $this->excludeConstraints,
            triggers: $this->triggers,
            unlogged: $this->unlogged,
            partitionStrategy: $this->partitionStrategy,
            partitionColumns: $this->partitionColumns,
            inherits: $this->inherits,
            tablespace: $this->tablespace,
        );
    }

    public function withInherits(string ...$inherits): self
    {
        return new self(
            schema: $this->schema,
            name: $this->name,
            columns: $this->columns,
            primaryKey: $this->primaryKey,
            indexes: $this->indexes,
            foreignKeys: $this->foreignKeys,
            uniqueConstraints: $this->uniqueConstraints,
            checkConstraints: $this->checkConstraints,
            excludeConstraints: $this->excludeConstraints,
            triggers: $this->triggers,
            unlogged: $this->unlogged,
            partitionStrategy: $this->partitionStrategy,
            partitionColumns: $this->partitionColumns,
            inherits: array_values($inherits),
            tablespace: $this->tablespace,
        );
    }

    public function withOptions(TableOptions $options): self
    {
        return new self(
            schema: $this->schema,
            name: $this->name,
            columns: $this->columns,
            primaryKey: $this->primaryKey,
            indexes: $this->indexes,
            foreignKeys: $options->foreignKeys,
            uniqueConstraints: $this->uniqueConstraints,
            checkConstraints: $options->checkConstraints,
            excludeConstraints: $options->excludeConstraints,
            triggers: $options->triggers,
            unlogged: $options->unlogged,
            partitionStrategy: $options->partitionStrategy,
            partitionColumns: $options->partitionColumns,
            inherits: $options->inherits,
            tablespace: $options->tablespace,
        );
    }

    public function withPartitionBy(PartitionStrategy $strategy, string ...$columns): self
    {
        return new self(
            schema: $this->schema,
            name: $this->name,
            columns: $this->columns,
            primaryKey: $this->primaryKey,
            indexes: $this->indexes,
            foreignKeys: $this->foreignKeys,
            uniqueConstraints: $this->uniqueConstraints,
            checkConstraints: $this->checkConstraints,
            excludeConstraints: $this->excludeConstraints,
            triggers: $this->triggers,
            unlogged: $this->unlogged,
            partitionStrategy: $strategy,
            partitionColumns: array_values($columns),
            inherits: $this->inherits,
            tablespace: $this->tablespace,
        );
    }

    public function withTablespace(?string $tablespace): self
    {
        return new self(
            schema: $this->schema,
            name: $this->name,
            columns: $this->columns,
            primaryKey: $this->primaryKey,
            indexes: $this->indexes,
            foreignKeys: $this->foreignKeys,
            uniqueConstraints: $this->uniqueConstraints,
            checkConstraints: $this->checkConstraints,
            excludeConstraints: $this->excludeConstraints,
            triggers: $this->triggers,
            unlogged: $this->unlogged,
            partitionStrategy: $this->partitionStrategy,
            partitionColumns: $this->partitionColumns,
            inherits: $this->inherits,
            tablespace: $tablespace,
        );
    }

    public function withTrigger(Trigger $trigger): self
    {
        return new self(
            schema: $this->schema,
            name: $this->name,
            columns: $this->columns,
            primaryKey: $this->primaryKey,
            indexes: $this->indexes,
            foreignKeys: $this->foreignKeys,
            uniqueConstraints: $this->uniqueConstraints,
            checkConstraints: $this->checkConstraints,
            excludeConstraints: $this->excludeConstraints,
            triggers: [...$this->triggers, $trigger],
            unlogged: $this->unlogged,
            partitionStrategy: $this->partitionStrategy,
            partitionColumns: $this->partitionColumns,
            inherits: $this->inherits,
            tablespace: $this->tablespace,
        );
    }

    public function withUnlogged(bool $unlogged = true): self
    {
        return new self(
            schema: $this->schema,
            name: $this->name,
            columns: $this->columns,
            primaryKey: $this->primaryKey,
            indexes: $this->indexes,
            foreignKeys: $this->foreignKeys,
            uniqueConstraints: $this->uniqueConstraints,
            checkConstraints: $this->checkConstraints,
            excludeConstraints: $this->excludeConstraints,
            triggers: $this->triggers,
            unlogged: $unlogged,
            partitionStrategy: $this->partitionStrategy,
            partitionColumns: $this->partitionColumns,
            inherits: $this->inherits,
            tablespace: $this->tablespace,
        );
    }

    /**
     * @return TableShape
     */
    public function normalize(): array
    {
        return [
            'schema' => $this->schema,
            'name' => $this->name,
            'columns' => array_map(static fn(Column $col): array => $col->normalize(), $this->columns),
            'primary_key' => $this->primaryKey?->normalize(),
            'indexes' => array_map(static fn(Index $idx): array => $idx->normalize(), $this->indexes),
            'foreign_keys' => array_map(static fn(ForeignKey $fk): array => $fk->normalize(), $this->foreignKeys),
            'unique_constraints' => array_map(
                static fn(UniqueConstraint $uc): array => $uc->normalize(),
                $this->uniqueConstraints,
            ),
            'check_constraints' => array_map(
                static fn(CheckConstraint $cc): array => $cc->normalize(),
                $this->checkConstraints,
            ),
            'exclude_constraints' => array_map(
                static fn(ExcludeConstraint $ec): array => $ec->normalize(),
                $this->excludeConstraints,
            ),
            'triggers' => array_map(static fn(Trigger $t): array => $t->normalize(), $this->triggers),
            'unlogged' => $this->unlogged,
            'partition_strategy' => $this->partitionStrategy?->value,
            'partition_columns' => $this->partitionColumns,
            'inherits' => $this->inherits,
            'tablespace' => $this->tablespace,
        ];
    }

    public function qualifiedName(): string
    {
        return $this->schema . '.' . $this->name;
    }

    /**
     * @return list<Sql>
     */
    public function toSql(): array
    {
        $sqls = [];

        $tableBuilder = create()->table($this->name, $this->schema);

        foreach ($this->columns as $col) {
            $colDef = column($col->name, $col->type);

            if (!$col->nullable) {
                $colDef = $colDef->notNull();
            }

            if ($col->default !== null) {
                $colDef = $colDef->defaultRaw(ExpressionFactory::fromAst(
                    (new ExpressionParser())->parse($col->default->applicableSql()),
                ));
            }

            if ($col->isIdentity) {
                $colDef = $colDef->identity($col->identityGeneration ?? IdentityGeneration::ALWAYS);
            }

            if ($col->isGenerated && $col->generationExpression !== null) {
                $colDef = $colDef->generatedAs(ExpressionFactory::fromAst((new ExpressionParser())->parse($col->generationExpression)));
            }

            $tableBuilder = $tableBuilder->column($colDef);
        }

        if ($this->primaryKey !== null) {
            $pk = PrimaryKeyConstraint::create(...$this->primaryKey->columns);

            if ($this->primaryKey->name !== null) {
                $pk = $pk->name($this->primaryKey->name);
            }
            $tableBuilder = $tableBuilder->constraint($pk);
        }

        foreach ($this->uniqueConstraints as $uc) {
            $constraint = UniqueConstraintBuilder::create(...$uc->columns);

            if ($uc->name !== null) {
                $constraint = $constraint->name($uc->name);
            }

            if ($uc->nullsNotDistinct) {
                $constraint = $constraint->nullsNotDistinct();
            }
            $tableBuilder = $tableBuilder->constraint($constraint);
        }

        foreach ($this->checkConstraints as $cc) {
            $constraint = CheckConstraintBuilder::create(ConditionFactory::fromAst((new ExpressionParser())->parse($cc->expression)));

            if ($cc->name !== null) {
                $constraint = $constraint->name($cc->name);
            }

            if ($cc->noInherit) {
                $constraint = $constraint->noInherit();
            }
            $tableBuilder = $tableBuilder->constraint($constraint);
        }

        foreach ($this->foreignKeys as $fk) {
            $refTable = $fk->referenceSchema . '.' . $fk->referenceTable;
            $constraint = ForeignKeyConstraint::create($fk->columns, $refTable, $fk->referenceColumns)
                ->onUpdate($fk->onUpdate)
                ->onDelete($fk->onDelete);

            if ($fk->name !== null) {
                $constraint = $constraint->name($fk->name);
            }

            if ($fk->deferrable) {
                $constraint = $constraint->deferrable($fk->initiallyDeferred);
            }
            $tableBuilder = $tableBuilder->constraint($constraint);
        }

        if ($this->unlogged) {
            $tableBuilder = $tableBuilder->unlogged();
        }

        if ($this->partitionStrategy !== null && $this->partitionColumns !== []) {
            $tableBuilder = match ($this->partitionStrategy) {
                PartitionStrategy::HASH => $tableBuilder->partitionByHash(...$this->partitionColumns),
                PartitionStrategy::LIST => $tableBuilder->partitionByList(...$this->partitionColumns),
                PartitionStrategy::RANGE => $tableBuilder->partitionByRange(...$this->partitionColumns),
            };
        }

        if ($this->inherits !== []) {
            $tableBuilder = $tableBuilder->inherits(...$this->inherits);
        }

        if ($this->tablespace !== null) {
            $tableBuilder = $tableBuilder->tablespace($this->tablespace);
        }

        $sqls[] = $tableBuilder;

        foreach ($this->indexes as $idx) {
            $builder = create()->index($idx->name);

            if ($idx->unique) {
                $builder = $builder->unique();
            }

            $onBuilder = $builder->on($this->name, $this->schema);

            if ($idx->method !== IndexMethod::BTREE) {
                $onBuilder = $onBuilder->using(QbIndexMethod::from($idx->method->value));
            }

            $sqls[] = $onBuilder->columns(...$idx->columns);
        }

        foreach ($this->triggers as $trigger) {
            $qbEvents = array_map(
                static fn(TriggerEvent $e): QbTriggerEvent => QbTriggerEvent::{$e->name},
                $trigger->events,
            );

            $triggerBuilder = create()->trigger($trigger->name);

            $onStep = match ($trigger->timing) {
                TriggerTiming::BEFORE => $triggerBuilder->before(...$qbEvents),
                TriggerTiming::AFTER => $triggerBuilder->after(...$qbEvents),
                TriggerTiming::INSTEAD_OF => $triggerBuilder->insteadOf(...$qbEvents),
            };

            $optionsStep = $onStep->on($this->name, $this->schema);

            if ($trigger->forEachRow) {
                $optionsStep = $optionsStep->forEachRow();
            }

            $sqls[] = $optionsStep->execute($trigger->functionName);
        }

        return $sqls;
    }
}
