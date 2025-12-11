<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Trigger;

use Flow\PgQuery\Protobuf\AST\{DropBehavior, DropStmt, Node, ObjectType, PBList, PBString};
use Flow\PgQuery\QueryBuilder\{AstToSql, QualifiedIdentifier};

final readonly class DropTriggerBuilder implements DropTriggerFinalStep, DropTriggerOnStep
{
    use AstToSql;

    private function __construct(
        private string $name,
        private bool $ifExists = false,
        private ?string $table = null,
        private ?string $schema = null,
        private int $behavior = DropBehavior::DROP_RESTRICT,
    ) {
    }

    public static function create(string $name) : DropTriggerOnStep
    {
        return new self($name);
    }

    public function cascade() : DropTriggerFinalStep
    {
        return new self(
            $this->name,
            $this->ifExists,
            $this->table,
            $this->schema,
            DropBehavior::DROP_CASCADE,
        );
    }

    public function ifExists() : DropTriggerOnStep
    {
        return new self(
            $this->name,
            true,
            $this->table,
            $this->schema,
            $this->behavior,
        );
    }

    public function on(string $table, ?string $schema = null) : DropTriggerFinalStep
    {
        if ($schema !== null) {
            return new self(
                $this->name,
                $this->ifExists,
                $table,
                $schema,
                $this->behavior,
            );
        }

        $identifier = QualifiedIdentifier::parse($table);

        return new self(
            $this->name,
            $this->ifExists,
            $identifier->name(),
            $identifier->schema(),
            $this->behavior,
        );
    }

    public function restrict() : DropTriggerFinalStep
    {
        return new self(
            $this->name,
            $this->ifExists,
            $this->table,
            $this->schema,
            DropBehavior::DROP_RESTRICT,
        );
    }

    public function toAst() : DropStmt
    {
        $stmt = new DropStmt();

        $stmt->setRemoveType(ObjectType::OBJECT_TRIGGER);
        $stmt->setMissingOk($this->ifExists);
        $stmt->setBehavior($this->behavior);

        $listItems = [];

        if ($this->schema !== null) {
            $schemaStr = new PBString();
            $schemaStr->setSval($this->schema);
            $schemaNode = new Node();
            $schemaNode->setString($schemaStr);
            $listItems[] = $schemaNode;
        }

        if ($this->table !== null) {
            $tableStr = new PBString();
            $tableStr->setSval($this->table);
            $tableNode = new Node();
            $tableNode->setString($tableStr);
            $listItems[] = $tableNode;
        }

        $nameStr = new PBString();
        $nameStr->setSval($this->name);
        $nameNode = new Node();
        $nameNode->setString($nameStr);
        $listItems[] = $nameNode;

        $list = new PBList();
        $list->setItems($listItems);

        $objectNode = new Node();
        $objectNode->setList($list);

        $stmt->setObjects([$objectNode]);

        return $stmt;
    }
}
