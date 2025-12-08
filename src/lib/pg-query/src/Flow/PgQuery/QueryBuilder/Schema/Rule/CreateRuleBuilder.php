<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Rule;

use Flow\PgQuery\Parser;
use Flow\PgQuery\Protobuf\AST\{Node, RangeVar, RuleStmt};
use Flow\PgQuery\QueryBuilder\Exception\InvalidAstException;

final readonly class CreateRuleBuilder implements CreateRuleDoStep, CreateRuleEventStep, CreateRuleFinalStep, CreateRuleToStep, CreateRuleWhereStep
{
    /**
     * @param list<Node> $actions
     */
    private function __construct(
        private string $name,
        private bool $replace = false,
        private ?RuleEvent $event = null,
        private ?string $table = null,
        private ?string $schema = null,
        private ?string $whereCondition = null,
        private bool $instead = false,
        private array $actions = [],
    ) {
    }

    public static function create(string $name) : CreateRuleEventStep
    {
        return new self($name);
    }

    public function asOnDelete() : CreateRuleToStep
    {
        return new self(
            $this->name,
            $this->replace,
            RuleEvent::DELETE,
            $this->table,
            $this->schema,
            $this->whereCondition,
            $this->instead,
            $this->actions,
        );
    }

    public function asOnInsert() : CreateRuleToStep
    {
        return new self(
            $this->name,
            $this->replace,
            RuleEvent::INSERT,
            $this->table,
            $this->schema,
            $this->whereCondition,
            $this->instead,
            $this->actions,
        );
    }

    public function asOnSelect() : CreateRuleToStep
    {
        return new self(
            $this->name,
            $this->replace,
            RuleEvent::SELECT,
            $this->table,
            $this->schema,
            $this->whereCondition,
            $this->instead,
            $this->actions,
        );
    }

    public function asOnUpdate() : CreateRuleToStep
    {
        return new self(
            $this->name,
            $this->replace,
            RuleEvent::UPDATE,
            $this->table,
            $this->schema,
            $this->whereCondition,
            $this->instead,
            $this->actions,
        );
    }

    public function doAlso(string $command) : CreateRuleFinalStep
    {
        return new self(
            $this->name,
            $this->replace,
            $this->event,
            $this->table,
            $this->schema,
            $this->whereCondition,
            false,
            [$this->parseCommand($command)],
        );
    }

    public function doInstead(string $command) : CreateRuleFinalStep
    {
        return new self(
            $this->name,
            $this->replace,
            $this->event,
            $this->table,
            $this->schema,
            $this->whereCondition,
            true,
            [$this->parseCommand($command)],
        );
    }

    public function doNothing() : CreateRuleFinalStep
    {
        return new self(
            $this->name,
            $this->replace,
            $this->event,
            $this->table,
            $this->schema,
            $this->whereCondition,
            false,
            [],
        );
    }

    public function orReplace() : CreateRuleEventStep
    {
        return new self(
            $this->name,
            true,
            $this->event,
            $this->table,
            $this->schema,
            $this->whereCondition,
            $this->instead,
            $this->actions,
        );
    }

    public function to(string $table, ?string $schema = null) : CreateRuleWhereStep
    {
        $parts = \explode('.', $table);

        if (\count($parts) === 2) {
            return new self(
                $this->name,
                $this->replace,
                $this->event,
                $parts[1],
                $parts[0],
                $this->whereCondition,
                $this->instead,
                $this->actions,
            );
        }

        return new self(
            $this->name,
            $this->replace,
            $this->event,
            $table,
            $schema,
            $this->whereCondition,
            $this->instead,
            $this->actions,
        );
    }

    public function toAst() : RuleStmt
    {
        $stmt = new RuleStmt();

        $stmt->setRulename($this->name);

        if ($this->replace) {
            $stmt->setReplace(true);
        }

        if ($this->event !== null) {
            $stmt->setEvent($this->event->value);
        }

        if ($this->table !== null) {
            $relation = new RangeVar();
            $relation->setRelname($this->table);
            $relation->setRelpersistence('p');
            $relation->setInh(true);

            if ($this->schema !== null) {
                $relation->setSchemaname($this->schema);
            }

            $stmt->setRelation($relation);
        }

        if ($this->whereCondition !== null) {
            $stmt->setWhereClause($this->parseCondition($this->whereCondition));
        }

        $stmt->setInstead($this->instead);

        if ($this->actions !== []) {
            $stmt->setActions($this->actions);
        }

        return $stmt;
    }

    public function where(string $condition) : CreateRuleDoStep
    {
        return new self(
            $this->name,
            $this->replace,
            $this->event,
            $this->table,
            $this->schema,
            $condition,
            $this->instead,
            $this->actions,
        );
    }

    private function parseCommand(string $command) : Node
    {
        $parser = new Parser();
        $parsed = $parser->parse($command);

        $stmts = $parsed->raw()->getStmts();

        if ($stmts === null || \count($stmts) === 0) {
            throw InvalidAstException::invalidFieldValue('stmts', 'ParseResult', 'expected at least one statement');
        }

        $stmt = $stmts[0]->getStmt();

        if ($stmt === null) {
            throw InvalidAstException::missingRequiredField('stmt', 'RawStmt');
        }

        return $stmt;
    }

    private function parseCondition(string $condition) : Node
    {
        $parser = new Parser();
        $parsed = $parser->parse("SELECT * FROM t WHERE {$condition}");

        $stmts = $parsed->raw()->getStmts();

        if ($stmts === null || \count($stmts) === 0) {
            throw InvalidAstException::invalidFieldValue('stmts', 'ParseResult', 'expected at least one statement');
        }

        $selectStmt = $stmts[0]->getStmt()?->getSelectStmt();

        if ($selectStmt === null) {
            throw InvalidAstException::unexpectedNodeType('SelectStmt', 'unknown');
        }

        $whereClause = $selectStmt->getWhereClause();

        if ($whereClause === null) {
            throw InvalidAstException::missingRequiredField('whereClause', 'SelectStmt');
        }

        return $whereClause;
    }
}
