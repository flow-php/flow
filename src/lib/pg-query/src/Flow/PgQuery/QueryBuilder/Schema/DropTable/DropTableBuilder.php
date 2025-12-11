<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\DropTable;

use Flow\PgQuery\Protobuf\AST\{DropBehavior, DropStmt, Node, ObjectType, PBString};
use Flow\PgQuery\Protobuf\AST\PBList;
use Flow\PgQuery\QueryBuilder\{AstToSql, QualifiedIdentifier};

final readonly class DropTableBuilder implements DropTableFinalStep
{
    use AstToSql;

    /**
     * @param list<string> $tables
     */
    private function __construct(
        private array $tables,
        private bool $ifExists = false,
        private int $behavior = DropBehavior::DROP_BEHAVIOR_UNDEFINED,
    ) {
    }

    public static function create(string ...$tables) : DropTableFinalStep
    {
        return new self(\array_values($tables));
    }

    public function cascade() : DropTableFinalStep
    {
        return new self(
            $this->tables,
            $this->ifExists,
            DropBehavior::DROP_CASCADE,
        );
    }

    public function ifExists() : DropTableFinalStep
    {
        return new self(
            $this->tables,
            true,
            $this->behavior,
        );
    }

    public function restrict() : DropTableFinalStep
    {
        return new self(
            $this->tables,
            $this->ifExists,
            DropBehavior::DROP_RESTRICT,
        );
    }

    public function toAst() : DropStmt
    {
        $stmt = new DropStmt();
        $stmt->setRemoveType(ObjectType::OBJECT_TABLE);

        if ($this->ifExists) {
            $stmt->setMissingOk(true);
        }

        if ($this->behavior !== DropBehavior::DROP_BEHAVIOR_UNDEFINED) {
            $stmt->setBehavior($this->behavior);
        }

        $objects = [];

        foreach ($this->tables as $table) {
            $objects[] = $this->createTableListNode($table);
        }

        $stmt->setObjects($objects);

        return $stmt;
    }

    private function createTableListNode(string $table) : Node
    {
        $identifier = QualifiedIdentifier::parse($table);
        $listItems = [];

        foreach ($identifier->parts() as $part) {
            $str = new PBString();
            $str->setSval($part);

            $strNode = new Node();
            $strNode->setString($str);
            $listItems[] = $strNode;
        }

        $list = new PBList();
        $list->setItems($listItems);

        $node = new Node();
        $node->setList($list);

        return $node;
    }
}
