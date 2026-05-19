<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\View\DropView;

use Flow\PostgreSql\Protobuf\AST\DropBehavior;
use Flow\PostgreSql\Protobuf\AST\DropStmt;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\ObjectType;
use Flow\PostgreSql\Protobuf\AST\PBList;
use Flow\PostgreSql\Protobuf\AST\PBString;
use Flow\PostgreSql\QueryBuilder\AstToSql;
use Flow\PostgreSql\QueryBuilder\QualifiedIdentifier;

use function array_values;

final readonly class DropViewBuilder implements DropViewFinalStep
{
    use AstToSql;

    /**
     * @param list<string> $views
     */
    private function __construct(
        private array $views,
        private bool $ifExists = false,
        private int $behavior = DropBehavior::DROP_BEHAVIOR_UNDEFINED,
    ) {}

    public static function create(string ...$views): DropViewFinalStep
    {
        return new self(array_values($views));
    }

    public function cascade(): DropViewFinalStep
    {
        return new self($this->views, $this->ifExists, DropBehavior::DROP_CASCADE);
    }

    public function ifExists(): DropViewFinalStep
    {
        return new self($this->views, true, $this->behavior);
    }

    public function restrict(): DropViewFinalStep
    {
        return new self($this->views, $this->ifExists, DropBehavior::DROP_RESTRICT);
    }

    public function toAst(): DropStmt
    {
        $stmt = new DropStmt();
        $stmt->setRemoveType(ObjectType::OBJECT_VIEW);

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

    private function createViewListNode(string $view): Node
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
