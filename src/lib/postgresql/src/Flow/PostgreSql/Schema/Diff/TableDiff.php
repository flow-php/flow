<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema\Diff;

use Flow\PostgreSql\Parser\ExcludeDefinitionParser;
use Flow\PostgreSql\Parser\ExpressionParser;
use Flow\PostgreSql\QueryBuilder\Condition\ConditionFactory;
use Flow\PostgreSql\QueryBuilder\Expression\ExpressionFactory;
use Flow\PostgreSql\QueryBuilder\Schema\Constraint\CheckConstraint as CheckConstraintBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Constraint\ExcludeConstraint as ExcludeConstraintBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Constraint\ForeignKeyConstraint;
use Flow\PostgreSql\QueryBuilder\Schema\Constraint\PrimaryKeyConstraint;
use Flow\PostgreSql\QueryBuilder\Schema\Constraint\UniqueConstraint as UniqueConstraintBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Index\IndexMethod as QbIndexMethod;
use Flow\PostgreSql\QueryBuilder\Schema\Trigger\TriggerEvent as QbTriggerEvent;
use Flow\PostgreSql\QueryBuilder\Sql;
use Flow\PostgreSql\Schema\Column;
use Flow\PostgreSql\Schema\Constraint\CheckConstraint;
use Flow\PostgreSql\Schema\Constraint\ExcludeConstraint;
use Flow\PostgreSql\Schema\Constraint\ForeignKey;
use Flow\PostgreSql\Schema\Constraint\PrimaryKey;
use Flow\PostgreSql\Schema\Constraint\UniqueConstraint;
use Flow\PostgreSql\Schema\IdentityGeneration;
use Flow\PostgreSql\Schema\Index;
use Flow\PostgreSql\Schema\IndexMethod;
use Flow\PostgreSql\Schema\Table;
use Flow\PostgreSql\Schema\Trigger;
use Flow\PostgreSql\Schema\TriggerEvent;
use Flow\PostgreSql\Schema\TriggerTiming;
use RuntimeException;

use function array_map;
use function Flow\PostgreSql\DSL\alter;
use function Flow\PostgreSql\DSL\column;
use function Flow\PostgreSql\DSL\create;
use function Flow\PostgreSql\DSL\drop;
use function sprintf;

final readonly class TableDiff implements Diff
{
    /**
     * @param list<Column> $addedColumns
     * @param list<Column> $removedColumns
     * @param list<ColumnDiff> $modifiedColumns
     * @param list<Index> $addedIndexes
     * @param list<Index> $removedIndexes
     * @param array<string, Index> $renamedIndexes
     * @param list<ForeignKey> $addedForeignKeys
     * @param list<ForeignKey> $removedForeignKeys
     * @param list<UniqueConstraint> $addedUniqueConstraints
     * @param list<UniqueConstraint> $removedUniqueConstraints
     * @param list<CheckConstraint> $addedCheckConstraints
     * @param list<CheckConstraint> $removedCheckConstraints
     * @param list<ExcludeConstraint> $addedExcludeConstraints
     * @param list<ExcludeConstraint> $removedExcludeConstraints
     * @param list<Trigger> $addedTriggers
     * @param list<Trigger> $removedTriggers
     * @param list<string> $addedInherits
     * @param list<string> $removedInherits
     */
    public function __construct(
        public Table $source,
        public Table $target,
        public array $addedColumns = [],
        public array $removedColumns = [],
        public array $modifiedColumns = [],
        public ?PrimaryKey $addedPrimaryKey = null,
        public ?PrimaryKey $removedPrimaryKey = null,
        public array $addedIndexes = [],
        public array $removedIndexes = [],
        public array $renamedIndexes = [],
        public array $addedForeignKeys = [],
        public array $removedForeignKeys = [],
        public array $addedUniqueConstraints = [],
        public array $removedUniqueConstraints = [],
        public array $addedCheckConstraints = [],
        public array $removedCheckConstraints = [],
        public array $addedExcludeConstraints = [],
        public array $removedExcludeConstraints = [],
        public array $addedTriggers = [],
        public array $removedTriggers = [],
        public bool $unloggedChanged = false,
        public bool $partitionChanged = false,
        public array $addedInherits = [],
        public array $removedInherits = [],
        public bool $tablespaceChanged = false,
    ) {}

    /**
     * @return list<Sql>
     */
    public function generate(): array
    {
        $sqls = [];
        $qualifiedName = $this->target->qualifiedName();

        if ($this->partitionChanged) {
            throw new RuntimeException(sprintf(
                'Partition strategy change on table "%s" cannot be applied via ALTER TABLE. The table must be recreated.',
                $qualifiedName,
            ));
        }

        if ($this->unloggedChanged) {
            $sqls[] = $this->target->unlogged
                ? alter()->table($qualifiedName)->setUnlogged()
                : alter()->table($qualifiedName)->setLogged();
        }

        foreach ($this->removedInherits as $parent) {
            $sqls[] = alter()->table($qualifiedName)->dropInherit($parent);
        }

        foreach ($this->removedForeignKeys as $fk) {
            if ($fk->name === null) {
                throw new RuntimeException(sprintf(
                    'Cannot drop unnamed foreign key on table "%s". Constraint names are required for reversible migrations.',
                    $qualifiedName,
                ));
            }
            $sqls[] = alter()->table($qualifiedName)->dropConstraint($fk->name);
        }

        foreach ($this->removedIndexes as $idx) {
            $sqls[] = drop()->index($idx->name);
        }

        foreach ($this->removedUniqueConstraints as $uc) {
            if ($uc->name === null) {
                throw new RuntimeException(sprintf(
                    'Cannot drop unnamed unique constraint on table "%s". Constraint names are required for reversible migrations.',
                    $qualifiedName,
                ));
            }
            $sqls[] = alter()->table($qualifiedName)->dropConstraint($uc->name);
        }

        foreach ($this->removedCheckConstraints as $cc) {
            if ($cc->name === null) {
                throw new RuntimeException(sprintf(
                    'Cannot drop unnamed check constraint on table "%s". Constraint names are required for reversible migrations.',
                    $qualifiedName,
                ));
            }
            $sqls[] = alter()->table($qualifiedName)->dropConstraint($cc->name);
        }

        foreach ($this->removedExcludeConstraints as $ec) {
            if ($ec->name === null) {
                throw new RuntimeException(sprintf(
                    'Cannot drop unnamed exclude constraint on table "%s". Constraint names are required for reversible migrations.',
                    $qualifiedName,
                ));
            }
            $sqls[] = alter()->table($qualifiedName)->dropConstraint($ec->name);
        }

        if ($this->removedPrimaryKey !== null) {
            if ($this->removedPrimaryKey->name === null) {
                throw new RuntimeException(sprintf(
                    'Cannot drop unnamed primary key on table "%s". Constraint names are required for reversible migrations.',
                    $qualifiedName,
                ));
            }
            $sqls[] = alter()->table($qualifiedName)->dropConstraint($this->removedPrimaryKey->name);
        }

        foreach ($this->removedTriggers as $trigger) {
            $sqls[] = drop()->trigger($trigger->name)->on($this->target->name, $this->target->schema);
        }

        foreach ($this->removedColumns as $col) {
            $sqls[] = alter()->table($qualifiedName)->dropColumn($col->name);
        }

        foreach ($this->addedColumns as $col) {
            $colDef = column($col->name, $col->type);

            if (!$col->nullable) {
                $colDef = $colDef->notNull();
            }

            if ($col->default !== null) {
                $colDef = $colDef->defaultRaw(ExpressionFactory::fromAst((new ExpressionParser())->parse($col->default)));
            }

            if ($col->isIdentity) {
                $colDef = $colDef->identity($col->identityGeneration ?? IdentityGeneration::ALWAYS);
            }

            if ($col->isGenerated && $col->generationExpression !== null) {
                $colDef = $colDef->generatedAs(ExpressionFactory::fromAst((new ExpressionParser())->parse($col->generationExpression)));
            }

            $sqls[] = alter()->table($qualifiedName)->addColumn($colDef);
        }

        foreach ($this->modifiedColumns as $colDiff) {
            $sqls = [...$sqls, ...$colDiff->generate()];
        }

        if ($this->addedPrimaryKey !== null) {
            $constraint = PrimaryKeyConstraint::create(...$this->addedPrimaryKey->columns);

            if ($this->addedPrimaryKey->name !== null) {
                $constraint = $constraint->name($this->addedPrimaryKey->name);
            }
            $sqls[] = alter()->table($qualifiedName)->addConstraint($constraint);
        }

        foreach ($this->addedIndexes as $idx) {
            $builder = create()->index($idx->name);

            if ($idx->unique) {
                $builder = $builder->unique();
            }

            $onBuilder = $builder->on($this->target->name, $this->target->schema);

            if ($idx->method !== IndexMethod::BTREE) {
                $onBuilder = $onBuilder->using(QbIndexMethod::from($idx->method->value));
            }

            $sqls[] = $onBuilder->columns(...$idx->columns);
        }

        foreach ($this->renamedIndexes as $oldName => $newIndex) {
            $sqls[] = alter()->index($oldName, $this->target->schema)->renameTo($newIndex->name);
        }

        foreach ($this->addedUniqueConstraints as $uc) {
            $constraint = UniqueConstraintBuilder::create(...$uc->columns);

            if ($uc->name !== null) {
                $constraint = $constraint->name($uc->name);
            }

            if ($uc->nullsNotDistinct) {
                $constraint = $constraint->nullsNotDistinct();
            }
            $sqls[] = alter()->table($qualifiedName)->addConstraint($constraint);
        }

        foreach ($this->addedCheckConstraints as $cc) {
            $constraint = CheckConstraintBuilder::create(ConditionFactory::fromAst((new ExpressionParser())->parse($cc->expression)));

            if ($cc->name !== null) {
                $constraint = $constraint->name($cc->name);
            }

            if ($cc->noInherit) {
                $constraint = $constraint->noInherit();
            }
            $sqls[] = alter()->table($qualifiedName)->addConstraint($constraint);
        }

        foreach ($this->addedExcludeConstraints as $ec) {
            $ecName = $ec->name;

            if ($ecName === null) {
                throw new RuntimeException(sprintf(
                    'Cannot add unnamed exclude constraint on table "%s". Constraint names are required for reversible migrations.',
                    $qualifiedName,
                ));
            }

            $expressionParser = new ExpressionParser();
            $parsed = (new ExcludeDefinitionParser($expressionParser))->parse($ec->definition);
            $constraint = ExcludeConstraintBuilder::create($parsed->accessMethod)->name($ecName);

            foreach ($parsed->elements as $element) {
                $constraint = $constraint->element(
                    ExpressionFactory::fromAst($expressionParser->parse($element['expression'])),
                    $element['operator'],
                );
            }

            if ($parsed->predicate !== null) {
                $constraint = $constraint->where(ConditionFactory::fromAst($expressionParser->parse($parsed->predicate)));
            }

            if ($parsed->deferrable) {
                $constraint = $constraint->deferrable($parsed->initiallyDeferred);
            }

            $sqls[] = alter()->table($qualifiedName)->addConstraint($constraint);
        }

        foreach ($this->addedForeignKeys as $fk) {
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

            $sqls[] = alter()->table($qualifiedName)->addConstraint($constraint);
        }

        foreach ($this->addedTriggers as $trigger) {
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

            $optionsStep = $onStep->on($this->target->name, $this->target->schema);

            if ($trigger->forEachRow) {
                $optionsStep = $optionsStep->forEachRow();
            }

            $sqls[] = $optionsStep->execute($trigger->functionName);
        }

        foreach ($this->addedInherits as $parent) {
            $sqls[] = alter()->table($qualifiedName)->addInherit($parent);
        }

        if ($this->tablespaceChanged && $this->target->tablespace !== null) {
            $sqls[] = alter()->table($qualifiedName)->setTablespace($this->target->tablespace);
        }

        return $sqls;
    }

    public function isEmpty(): bool
    {
        return (
            $this->addedColumns === []
            && $this->removedColumns === []
            && $this->modifiedColumns === []
            && $this->addedPrimaryKey === null
            && $this->removedPrimaryKey === null
            && $this->addedIndexes === []
            && $this->removedIndexes === []
            && $this->renamedIndexes === []
            && $this->addedForeignKeys === []
            && $this->removedForeignKeys === []
            && $this->addedUniqueConstraints === []
            && $this->removedUniqueConstraints === []
            && $this->addedCheckConstraints === []
            && $this->removedCheckConstraints === []
            && $this->addedExcludeConstraints === []
            && $this->removedExcludeConstraints === []
            && $this->addedTriggers === []
            && $this->removedTriggers === []
            && !$this->unloggedChanged
            && !$this->partitionChanged
            && $this->addedInherits === []
            && $this->removedInherits === []
            && !$this->tablespaceChanged
        );
    }

    public function requiresViewRebuild(): bool
    {
        if ($this->removedColumns !== []) {
            return true;
        }

        foreach ($this->modifiedColumns as $colDiff) {
            if ($colDiff->hasTypeChanged() || $colDiff->hasNameChanged()) {
                return true;
            }
        }

        return false;
    }
}
