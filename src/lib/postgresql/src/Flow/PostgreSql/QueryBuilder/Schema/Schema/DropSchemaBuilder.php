<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Schema;

use Flow\PostgreSql\Protobuf\AST\DropBehavior;
use Flow\PostgreSql\Protobuf\AST\DropStmt;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\ObjectType;
use Flow\PostgreSql\Protobuf\AST\PBString;
use Flow\PostgreSql\QueryBuilder\AstToSql;

use function array_values;

final readonly class DropSchemaBuilder implements DropSchemaFinalStep
{
    use AstToSql;

    /**
     * @param list<string> $names
     */
    private function __construct(
        private array $names,
        private bool $ifExists = false,
        private int $behavior = DropBehavior::DROP_RESTRICT,
    ) {}

    public static function create(string ...$names): DropSchemaFinalStep
    {
        return new self(array_values($names));
    }

    public function cascade(): DropSchemaFinalStep
    {
        return new self($this->names, $this->ifExists, DropBehavior::DROP_CASCADE);
    }

    public function ifExists(): DropSchemaFinalStep
    {
        return new self($this->names, true, $this->behavior);
    }

    public function restrict(): DropSchemaFinalStep
    {
        return new self($this->names, $this->ifExists, DropBehavior::DROP_RESTRICT);
    }

    public function toAst(): DropStmt
    {
        $stmt = new DropStmt();
        $stmt->setRemoveType(ObjectType::OBJECT_SCHEMA);
        $stmt->setBehavior($this->behavior);
        $stmt->setMissingOk($this->ifExists);

        $objects = [];

        foreach ($this->names as $name) {
            $str = new PBString();
            $str->setSval($name);
            $node = new Node();
            $node->setString($str);
            $objects[] = $node;
        }

        $stmt->setObjects($objects);

        return $stmt;
    }
}
