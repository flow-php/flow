<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\DropSequence;

use Flow\PgQuery\Protobuf\AST\{DropBehavior, DropStmt, Node, ObjectType, PBList, PBString};

final readonly class DropSequenceBuilder implements DropSequenceFinalStep, DropSequenceNameStep
{
    /**
     * @param list<string> $sequences
     */
    private function __construct(
        private array $sequences = [],
        private bool $ifExists = false,
        private int $behavior = DropBehavior::DROP_BEHAVIOR_UNDEFINED,
    ) {
    }

    public static function create() : DropSequenceNameStep
    {
        return new self();
    }

    public static function ifExists() : DropSequenceNameStep
    {
        return new self(ifExists: true);
    }

    public function cascade() : DropSequenceFinalStep
    {
        return new self(
            $this->sequences,
            $this->ifExists,
            DropBehavior::DROP_CASCADE,
        );
    }

    public function restrict() : DropSequenceFinalStep
    {
        return new self(
            $this->sequences,
            $this->ifExists,
            DropBehavior::DROP_RESTRICT,
        );
    }

    public function sequence(string ...$names) : DropSequenceFinalStep
    {
        return new self(
            \array_values($names),
            $this->ifExists,
            $this->behavior,
        );
    }

    public function toAst() : DropStmt
    {
        $stmt = new DropStmt();
        $stmt->setRemoveType(ObjectType::OBJECT_SEQUENCE);

        if ($this->ifExists) {
            $stmt->setMissingOk(true);
        }

        if ($this->behavior !== DropBehavior::DROP_BEHAVIOR_UNDEFINED) {
            $stmt->setBehavior($this->behavior);
        }

        $objects = [];

        foreach ($this->sequences as $sequence) {
            $objects[] = $this->createSequenceListNode($sequence);
        }

        $stmt->setObjects($objects);

        return $stmt;
    }

    private function createSequenceListNode(string $sequence) : Node
    {
        $parts = \explode('.', $sequence);
        $listItems = [];

        foreach ($parts as $part) {
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
