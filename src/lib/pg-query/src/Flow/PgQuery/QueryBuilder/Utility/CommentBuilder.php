<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Utility;

use Flow\PgQuery\Protobuf\AST\{CommentStmt, Node, ObjectType, PBList, PBString};
use Flow\PgQuery\QueryBuilder\{AstToSql, QualifiedIdentifier};

final readonly class CommentBuilder implements CommentFinalStep
{
    use AstToSql;

    private function __construct(
        private CommentTarget $target,
        private string $name,
        private ?string $comment = null,
    ) {
    }

    public static function create(CommentTarget $target, string $name) : CommentFinalStep
    {
        return new self($target, $name);
    }

    public function is(string $comment) : CommentFinalStep
    {
        return new self($this->target, $this->name, $comment);
    }

    public function isNull() : CommentFinalStep
    {
        return new self($this->target, $this->name, null);
    }

    public function toAst() : CommentStmt
    {
        $stmt = new CommentStmt();
        $stmt->setObjtype($this->target->value);

        $objectNode = $this->buildObjectNode();
        $stmt->setObject($objectNode);

        if ($this->comment !== null) {
            $stmt->setComment($this->comment);
        }

        return $stmt;
    }

    private function buildObjectNode() : Node
    {
        $node = new Node();

        if ($this->target === CommentTarget::COLUMN) {
            $identifier = QualifiedIdentifier::parse($this->name);
            $list = new PBList();
            $items = [];

            foreach ($identifier->parts() as $part) {
                $str = new PBString();
                $str->setSval($part);
                $strNode = new Node();
                $strNode->setString($str);
                $items[] = $strNode;
            }

            $list->setItems($items);
            $node->setList($list);
        } elseif (\in_array($this->target, [CommentTarget::FUNCTION, CommentTarget::PROCEDURE], true)) {
            $identifier = QualifiedIdentifier::parse($this->name);
            $list = new PBList();
            $items = [];

            foreach ($identifier->parts() as $part) {
                $str = new PBString();
                $str->setSval($part);
                $strNode = new Node();
                $strNode->setString($str);
                $items[] = $strNode;
            }

            $list->setItems($items);
            $node->setList($list);
        } elseif (\in_array($this->target->value, [
            ObjectType::OBJECT_TABLE,
            ObjectType::OBJECT_INDEX,
            ObjectType::OBJECT_SEQUENCE,
            ObjectType::OBJECT_VIEW,
            ObjectType::OBJECT_MATVIEW,
            ObjectType::OBJECT_TRIGGER,
            ObjectType::OBJECT_TYPE,
        ], true)) {
            $identifier = QualifiedIdentifier::parse($this->name);
            $list = new PBList();
            $items = [];

            foreach ($identifier->parts() as $part) {
                $str = new PBString();
                $str->setSval($part);
                $strNode = new Node();
                $strNode->setString($str);
                $items[] = $strNode;
            }

            $list->setItems($items);
            $node->setList($list);
        } else {
            $str = new PBString();
            $str->setSval($this->name);
            $node->setString($str);
        }

        return $node;
    }
}
