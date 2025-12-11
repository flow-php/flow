<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Trigger;

use Flow\PgQuery\Protobuf\AST\{CreateTrigStmt, Node, PBString, RangeVar, TriggerTransition};
use Flow\PgQuery\QueryBuilder\{AstToSql, QualifiedIdentifier};
use Flow\PgQuery\QueryBuilder\Condition\Condition;
use Flow\PgQuery\QueryBuilder\Expression\Expression;

final readonly class CreateTriggerBuilder implements CreateTriggerFinalStep, CreateTriggerOnStep, CreateTriggerOptionsStep, CreateTriggerTimingStep
{
    use AstToSql;

    /**
     * @param list<TriggerEvent> $events
     * @param list<string> $columns
     * @param list<array{name: string, isNew: bool}> $transitionTables
     * @param list<Expression> $functionArgs
     */
    private function __construct(
        private string $name,
        private bool $replace = false,
        private bool $constraint = false,
        private ?TriggerTiming $timing = null,
        private array $events = [],
        private array $columns = [],
        private ?string $table = null,
        private ?string $schema = null,
        private ?string $fromTable = null,
        private bool $deferrable = false,
        private bool $initDeferred = false,
        private array $transitionTables = [],
        private TriggerLevel $level = TriggerLevel::STATEMENT,
        private ?Condition $when = null,
        private ?string $functionName = null,
        private array $functionArgs = [],
    ) {
    }

    public static function create(string $name) : CreateTriggerTimingStep
    {
        return new self($name);
    }

    public function after(TriggerEvent ...$events) : CreateTriggerOnStep
    {
        return new self(
            $this->name,
            $this->replace,
            $this->constraint,
            TriggerTiming::AFTER,
            \array_values($events),
            [],
            $this->table,
            $this->schema,
            $this->fromTable,
            $this->deferrable,
            $this->initDeferred,
            $this->transitionTables,
            $this->level,
            $this->when,
            $this->functionName,
            $this->functionArgs,
        );
    }

    public function afterUpdateOf(string ...$columns) : CreateTriggerOnStep
    {
        return new self(
            $this->name,
            $this->replace,
            $this->constraint,
            TriggerTiming::AFTER,
            [TriggerEvent::UPDATE],
            \array_values($columns),
            $this->table,
            $this->schema,
            $this->fromTable,
            $this->deferrable,
            $this->initDeferred,
            $this->transitionTables,
            $this->level,
            $this->when,
            $this->functionName,
            $this->functionArgs,
        );
    }

    public function before(TriggerEvent ...$events) : CreateTriggerOnStep
    {
        return new self(
            $this->name,
            $this->replace,
            $this->constraint,
            TriggerTiming::BEFORE,
            \array_values($events),
            [],
            $this->table,
            $this->schema,
            $this->fromTable,
            $this->deferrable,
            $this->initDeferred,
            $this->transitionTables,
            $this->level,
            $this->when,
            $this->functionName,
            $this->functionArgs,
        );
    }

    public function beforeUpdateOf(string ...$columns) : CreateTriggerOnStep
    {
        return new self(
            $this->name,
            $this->replace,
            $this->constraint,
            TriggerTiming::BEFORE,
            [TriggerEvent::UPDATE],
            \array_values($columns),
            $this->table,
            $this->schema,
            $this->fromTable,
            $this->deferrable,
            $this->initDeferred,
            $this->transitionTables,
            $this->level,
            $this->when,
            $this->functionName,
            $this->functionArgs,
        );
    }

    public function constraint() : CreateTriggerTimingStep
    {
        return new self(
            $this->name,
            $this->replace,
            true,
            $this->timing,
            $this->events,
            $this->columns,
            $this->table,
            $this->schema,
            $this->fromTable,
            $this->deferrable,
            $this->initDeferred,
            $this->transitionTables,
            $this->level,
            $this->when,
            $this->functionName,
            $this->functionArgs,
        );
    }

    public function deferrable() : CreateTriggerOptionsStep
    {
        return new self(
            $this->name,
            $this->replace,
            $this->constraint,
            $this->timing,
            $this->events,
            $this->columns,
            $this->table,
            $this->schema,
            $this->fromTable,
            true,
            $this->initDeferred,
            $this->transitionTables,
            $this->level,
            $this->when,
            $this->functionName,
            $this->functionArgs,
        );
    }

    public function execute(string $functionName, Expression ...$args) : CreateTriggerFinalStep
    {
        return new self(
            $this->name,
            $this->replace,
            $this->constraint,
            $this->timing,
            $this->events,
            $this->columns,
            $this->table,
            $this->schema,
            $this->fromTable,
            $this->deferrable,
            $this->initDeferred,
            $this->transitionTables,
            $this->level,
            $this->when,
            $functionName,
            \array_values($args),
        );
    }

    public function forEachRow() : CreateTriggerOptionsStep
    {
        return new self(
            $this->name,
            $this->replace,
            $this->constraint,
            $this->timing,
            $this->events,
            $this->columns,
            $this->table,
            $this->schema,
            $this->fromTable,
            $this->deferrable,
            $this->initDeferred,
            $this->transitionTables,
            TriggerLevel::ROW,
            $this->when,
            $this->functionName,
            $this->functionArgs,
        );
    }

    public function forEachStatement() : CreateTriggerOptionsStep
    {
        return new self(
            $this->name,
            $this->replace,
            $this->constraint,
            $this->timing,
            $this->events,
            $this->columns,
            $this->table,
            $this->schema,
            $this->fromTable,
            $this->deferrable,
            $this->initDeferred,
            $this->transitionTables,
            TriggerLevel::STATEMENT,
            $this->when,
            $this->functionName,
            $this->functionArgs,
        );
    }

    public function from(string $referencedTable) : CreateTriggerOptionsStep
    {
        return new self(
            $this->name,
            $this->replace,
            $this->constraint,
            $this->timing,
            $this->events,
            $this->columns,
            $this->table,
            $this->schema,
            $referencedTable,
            $this->deferrable,
            $this->initDeferred,
            $this->transitionTables,
            $this->level,
            $this->when,
            $this->functionName,
            $this->functionArgs,
        );
    }

    public function initiallyDeferred() : CreateTriggerOptionsStep
    {
        return new self(
            $this->name,
            $this->replace,
            $this->constraint,
            $this->timing,
            $this->events,
            $this->columns,
            $this->table,
            $this->schema,
            $this->fromTable,
            $this->deferrable,
            true,
            $this->transitionTables,
            $this->level,
            $this->when,
            $this->functionName,
            $this->functionArgs,
        );
    }

    public function initiallyImmediate() : CreateTriggerOptionsStep
    {
        return new self(
            $this->name,
            $this->replace,
            $this->constraint,
            $this->timing,
            $this->events,
            $this->columns,
            $this->table,
            $this->schema,
            $this->fromTable,
            $this->deferrable,
            false,
            $this->transitionTables,
            $this->level,
            $this->when,
            $this->functionName,
            $this->functionArgs,
        );
    }

    public function insteadOf(TriggerEvent ...$events) : CreateTriggerOnStep
    {
        return new self(
            $this->name,
            $this->replace,
            $this->constraint,
            TriggerTiming::INSTEAD_OF,
            \array_values($events),
            [],
            $this->table,
            $this->schema,
            $this->fromTable,
            $this->deferrable,
            $this->initDeferred,
            $this->transitionTables,
            $this->level,
            $this->when,
            $this->functionName,
            $this->functionArgs,
        );
    }

    public function notDeferrable() : CreateTriggerOptionsStep
    {
        return new self(
            $this->name,
            $this->replace,
            $this->constraint,
            $this->timing,
            $this->events,
            $this->columns,
            $this->table,
            $this->schema,
            $this->fromTable,
            false,
            false,
            $this->transitionTables,
            $this->level,
            $this->when,
            $this->functionName,
            $this->functionArgs,
        );
    }

    public function on(string $table, ?string $schema = null) : CreateTriggerOptionsStep
    {
        $identifier = QualifiedIdentifier::parse($table);

        return new self(
            $this->name,
            $this->replace,
            $this->constraint,
            $this->timing,
            $this->events,
            $this->columns,
            $identifier->name(),
            $schema ?? $identifier->schema(),
            $this->fromTable,
            $this->deferrable,
            $this->initDeferred,
            $this->transitionTables,
            $this->level,
            $this->when,
            $this->functionName,
            $this->functionArgs,
        );
    }

    public function orReplace() : CreateTriggerTimingStep
    {
        return new self(
            $this->name,
            true,
            $this->constraint,
            $this->timing,
            $this->events,
            $this->columns,
            $this->table,
            $this->schema,
            $this->fromTable,
            $this->deferrable,
            $this->initDeferred,
            $this->transitionTables,
            $this->level,
            $this->when,
            $this->functionName,
            $this->functionArgs,
        );
    }

    public function referencingNewTableAs(string $name) : CreateTriggerOptionsStep
    {
        return new self(
            $this->name,
            $this->replace,
            $this->constraint,
            $this->timing,
            $this->events,
            $this->columns,
            $this->table,
            $this->schema,
            $this->fromTable,
            $this->deferrable,
            $this->initDeferred,
            [...$this->transitionTables, ['name' => $name, 'isNew' => true]],
            $this->level,
            $this->when,
            $this->functionName,
            $this->functionArgs,
        );
    }

    public function referencingOldTableAs(string $name) : CreateTriggerOptionsStep
    {
        return new self(
            $this->name,
            $this->replace,
            $this->constraint,
            $this->timing,
            $this->events,
            $this->columns,
            $this->table,
            $this->schema,
            $this->fromTable,
            $this->deferrable,
            $this->initDeferred,
            [...$this->transitionTables, ['name' => $name, 'isNew' => false]],
            $this->level,
            $this->when,
            $this->functionName,
            $this->functionArgs,
        );
    }

    public function toAst() : CreateTrigStmt
    {
        $stmt = new CreateTrigStmt();

        $stmt->setTrigname($this->name);
        $stmt->setReplace($this->replace);
        $stmt->setIsconstraint($this->constraint);

        if ($this->timing !== null) {
            $stmt->setTiming($this->timing->value);
        }

        $eventsMask = 0;

        foreach ($this->events as $event) {
            $eventsMask |= $event->value;
        }
        $stmt->setEvents($eventsMask);

        if ($this->columns !== []) {
            $columnNodes = [];

            foreach ($this->columns as $column) {
                $str = new PBString();
                $str->setSval($column);
                $node = new Node();
                $node->setString($str);
                $columnNodes[] = $node;
            }
            $stmt->setColumns($columnNodes);
        }

        if ($this->table !== null) {
            $rangeVar = new RangeVar();
            $rangeVar->setRelname($this->table);
            $rangeVar->setRelpersistence('p');
            $rangeVar->setInh(true);

            if ($this->schema !== null) {
                $rangeVar->setSchemaname($this->schema);
            }
            $stmt->setRelation($rangeVar);
        }

        if ($this->fromTable !== null) {
            $constrrel = new RangeVar();
            $constrrel->setRelname($this->fromTable);
            $constrrel->setRelpersistence('p');
            $constrrel->setInh(true);
            $stmt->setConstrrel($constrrel);
        }

        $stmt->setDeferrable($this->deferrable);
        $stmt->setInitdeferred($this->initDeferred);

        if ($this->transitionTables !== []) {
            $transitionNodes = [];

            foreach ($this->transitionTables as $trans) {
                $transition = new TriggerTransition();
                $transition->setName($trans['name']);
                $transition->setIsNew($trans['isNew']);
                $transition->setIsTable(true);

                $node = new Node();
                $node->setTriggerTransition($transition);
                $transitionNodes[] = $node;
            }
            $stmt->setTransitionRels($transitionNodes);
        }

        $stmt->setRow($this->level->toBool());

        if ($this->when !== null) {
            $stmt->setWhenClause($this->when->toAst());
        }

        if ($this->functionName !== null) {
            $funcIdentifier = QualifiedIdentifier::parse($this->functionName);
            $funcnameNodes = [];

            foreach ($funcIdentifier->parts() as $part) {
                $str = new PBString();
                $str->setSval($part);
                $node = new Node();
                $node->setString($str);
                $funcnameNodes[] = $node;
            }
            $stmt->setFuncname($funcnameNodes);
        }

        if ($this->functionArgs !== []) {
            $argNodes = [];

            foreach ($this->functionArgs as $arg) {
                $argNodes[] = $arg->toAst();
            }
            $stmt->setArgs($argNodes);
        }

        return $stmt;
    }

    public function when(Condition $condition) : CreateTriggerOptionsStep
    {
        return new self(
            $this->name,
            $this->replace,
            $this->constraint,
            $this->timing,
            $this->events,
            $this->columns,
            $this->table,
            $this->schema,
            $this->fromTable,
            $this->deferrable,
            $this->initDeferred,
            $this->transitionTables,
            $this->level,
            $condition,
            $this->functionName,
            $this->functionArgs,
        );
    }
}
