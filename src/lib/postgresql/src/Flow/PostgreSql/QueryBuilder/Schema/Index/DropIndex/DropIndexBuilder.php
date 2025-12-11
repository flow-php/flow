<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Index\DropIndex;

use Flow\PostgreSql\Protobuf\AST\{DropBehavior, DropStmt, Node, ObjectType, PBString};
use Flow\PostgreSql\Protobuf\AST\PBList;
use Flow\PostgreSql\QueryBuilder\{AstToSql, QualifiedIdentifier};

final readonly class DropIndexBuilder implements DropIndexFinalStep
{
    use AstToSql;

    /**
     * @param list<string> $indexes
     */
    private function __construct(
        private array $indexes,
        private bool $ifExists = false,
        private bool $concurrent = false,
        private int $behavior = DropBehavior::DROP_BEHAVIOR_UNDEFINED,
    ) {
    }

    public static function create(string ...$indexes) : DropIndexFinalStep
    {
        return new self(\array_values($indexes));
    }

    public function cascade() : DropIndexFinalStep
    {
        return new self(
            $this->indexes,
            $this->ifExists,
            $this->concurrent,
            DropBehavior::DROP_CASCADE,
        );
    }

    public function concurrently() : DropIndexFinalStep
    {
        return new self(
            $this->indexes,
            $this->ifExists,
            true,
            $this->behavior,
        );
    }

    public function ifExists() : DropIndexFinalStep
    {
        return new self(
            $this->indexes,
            true,
            $this->concurrent,
            $this->behavior,
        );
    }

    public function restrict() : DropIndexFinalStep
    {
        return new self(
            $this->indexes,
            $this->ifExists,
            $this->concurrent,
            DropBehavior::DROP_RESTRICT,
        );
    }

    public function toAst() : DropStmt
    {
        $stmt = new DropStmt();
        $stmt->setRemoveType(ObjectType::OBJECT_INDEX);

        if ($this->ifExists) {
            $stmt->setMissingOk(true);
        }

        if ($this->concurrent) {
            $stmt->setConcurrent(true);
        }

        if ($this->behavior !== DropBehavior::DROP_BEHAVIOR_UNDEFINED) {
            $stmt->setBehavior($this->behavior);
        }

        $objects = [];

        foreach ($this->indexes as $index) {
            $objects[] = $this->createIndexListNode($index);
        }

        $stmt->setObjects($objects);

        return $stmt;
    }

    private function createIndexListNode(string $index) : Node
    {
        $identifier = QualifiedIdentifier::parse($index);
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
