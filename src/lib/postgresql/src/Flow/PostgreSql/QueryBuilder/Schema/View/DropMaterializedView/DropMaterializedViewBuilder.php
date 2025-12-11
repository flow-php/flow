<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\View\DropMaterializedView;

use Flow\PostgreSql\Protobuf\AST\{DropBehavior, DropStmt, Node, ObjectType, PBList, PBString};
use Flow\PostgreSql\QueryBuilder\{AstToSql, QualifiedIdentifier};

final readonly class DropMaterializedViewBuilder implements DropMatViewFinalStep
{
    use AstToSql;

    /**
     * @param list<string> $views
     */
    private function __construct(
        private array $views,
        private bool $ifExists = false,
        private int $behavior = DropBehavior::DROP_BEHAVIOR_UNDEFINED,
    ) {
    }

    public static function create(string ...$views) : DropMatViewFinalStep
    {
        return new self(\array_values($views));
    }

    public function cascade() : DropMatViewFinalStep
    {
        return new self(
            $this->views,
            $this->ifExists,
            DropBehavior::DROP_CASCADE,
        );
    }

    public function ifExists() : DropMatViewFinalStep
    {
        return new self(
            $this->views,
            true,
            $this->behavior,
        );
    }

    public function restrict() : DropMatViewFinalStep
    {
        return new self(
            $this->views,
            $this->ifExists,
            DropBehavior::DROP_RESTRICT,
        );
    }

    public function toAst() : DropStmt
    {
        $stmt = new DropStmt();
        $stmt->setRemoveType(ObjectType::OBJECT_MATVIEW);

        if ($this->ifExists) {
            $stmt->setMissingOk(true);
        }

        if ($this->behavior !== DropBehavior::DROP_BEHAVIOR_UNDEFINED) {
            $stmt->setBehavior($this->behavior);
        }

        $objects = [];

        foreach ($this->views as $view) {
            $objects[] = $this->createViewListNode($view);
        }

        $stmt->setObjects($objects);

        return $stmt;
    }

    private function createViewListNode(string $view) : Node
    {
        $identifier = QualifiedIdentifier::parse($view);
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
