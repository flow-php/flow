<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Rule;

use Flow\PostgreSql\Parser;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\RangeVar;
use Flow\PostgreSql\Protobuf\AST\RuleStmt;
use Flow\PostgreSql\QueryBuilder\AstToSql;
use Flow\PostgreSql\QueryBuilder\Condition\Condition;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;
use Flow\PostgreSql\QueryBuilder\QualifiedIdentifier;

use function count;

final readonly class CreateRuleBuilder implements
    CreateRuleDoStep,
    CreateRuleEventStep,
    CreateRuleFinalStep,
    CreateRuleToStep,
    CreateRuleWhereStep
{
    use AstToSql;

    /**
     * @param list<Node> $actions
     */
    private function __construct(
        private string $name,
        private bool $replace = false,
        private ?RuleEvent $event = null,
        private ?string $table = null,
        private ?string $schema = null,
        private ?Condition $whereCondition = null,
        private bool $instead = false,
        private array $actions = [],
    ) {}

    public static function create(string $name): CreateRuleEventStep
    {
        return new self($name);
    }

    public function asOnDelete(): CreateRuleToStep
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

    public function asOnInsert(): CreateRuleToStep
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

    public function asOnSelect(): CreateRuleToStep
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

    public function asOnUpdate(): CreateRuleToStep
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

    public function doAlso(string $command): CreateRuleFinalStep
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

    public function doInstead(string $command): CreateRuleFinalStep
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

    public function doNothing(): CreateRuleFinalStep
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

    public function orReplace(): CreateRuleEventStep
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

    public function to(string $table, ?string $schema = null): CreateRuleWhereStep
    {
        $identifier = QualifiedIdentifier::parse($table);

        return new self(
            $this->name,
            $this->replace,
            $this->event,
            $identifier->name(),
            $schema ?? $identifier->schema(),
            $this->whereCondition,
            $this->instead,
            $this->actions,
        );
    }

    public function toAst(): RuleStmt
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
            $stmt->setWhereClause($this->whereCondition->toAst());
        }

        $stmt->setInstead($this->instead);

        if ($this->actions !== []) {
            $stmt->setActions($this->actions);
        }

        return $stmt;
    }

    public function where(Condition $condition): CreateRuleDoStep
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

    private function parseCommand(string $command): Node
    {
        $parser = new Parser();
        $parsed = $parser->parse($command);

        $stmts = $parsed->raw()->getStmts();

        if (count($stmts) === 0) {
            throw InvalidAstException::invalidFieldValue('stmts', 'ParseResult', 'expected at least one statement');
        }

        $stmt = $stmts[0]->getStmt();

        if ($stmt === null) {
            throw InvalidAstException::missingRequiredField('stmt', 'RawStmt');
        }

        return $stmt;
    }
}
