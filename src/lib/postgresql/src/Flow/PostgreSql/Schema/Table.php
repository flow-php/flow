<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema;

use function Flow\PostgreSql\DSL\{column, create};

use Flow\PostgreSql\Parser;
use Flow\PostgreSql\Parser\ExpressionParser;
use Flow\PostgreSql\QueryBuilder\Condition\ConditionFactory;
use Flow\PostgreSql\QueryBuilder\Expression\ExpressionFactory;
use Flow\PostgreSql\QueryBuilder\Schema\Constraint\{CheckConstraint as CheckConstraintBuilder, ForeignKeyConstraint, PrimaryKeyConstraint, UniqueConstraint as UniqueConstraintBuilder};
use Flow\PostgreSql\QueryBuilder\Schema\Index\IndexMethod as QbIndexMethod;
use Flow\PostgreSql\QueryBuilder\Schema\Trigger\TriggerEvent as QbTriggerEvent;
use Flow\PostgreSql\QueryBuilder\SqlQuery;
use Flow\PostgreSql\Schema\Constraint\{CheckConstraint, ExcludeConstraint, ForeignKey, PrimaryKey, UniqueConstraint};
use Flow\PostgreSql\Schema\Exception\ColumnNotFoundException;

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
    ) {
    }

    public function column(string $name) : Column
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
    public function columnNames() : array
    {
        return \array_map(
            static fn (Column $c) : string => $c->name,
            $this->columns,
        );
    }

    public function hasColumn(string $name) : bool
    {
        foreach ($this->columns as $column) {
            if ($column->name === $name) {
                return true;
            }
        }

        return false;
    }

    public function qualifiedName() : string
    {
        return $this->schema . '.' . $this->name;
    }

    /**
     * @return list<SqlQuery>
     */
    public function toSql() : array
    {
        $sqls = [];

        $tableBuilder = create()->table($this->name, $this->schema);

        foreach ($this->columns as $col) {
            $colDef = column($col->name, $col->type);

            if (!$col->nullable) {
                $colDef = $colDef->notNull();
            }

            if ($col->default !== null) {
                $colDef = $colDef->defaultRaw(ExpressionFactory::fromAst((new ExpressionParser(new Parser()))->parse($col->default)));
            }

            if ($col->isIdentity) {
                $colDef = $colDef->identity($col->identityGeneration ?? IdentityGeneration::ALWAYS);
            }

            if ($col->isGenerated && $col->generationExpression !== null) {
                $colDef = $colDef->generatedAs(ExpressionFactory::fromAst((new ExpressionParser(new Parser()))->parse($col->generationExpression)));
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
            $constraint = CheckConstraintBuilder::create(ConditionFactory::fromAst((new ExpressionParser(new Parser()))->parse($cc->expression)));

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
            $qbEvents = \array_map(
                static fn (TriggerEvent $e) : QbTriggerEvent => QbTriggerEvent::{$e->name},
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
