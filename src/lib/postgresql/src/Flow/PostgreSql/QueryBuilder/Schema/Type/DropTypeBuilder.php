<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Type;

use Flow\PostgreSql\Protobuf\AST\DropBehavior;
use Flow\PostgreSql\Protobuf\AST\DropStmt;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\ObjectType;
use Flow\PostgreSql\Protobuf\AST\PBString;
use Flow\PostgreSql\Protobuf\AST\TypeName;
use Flow\PostgreSql\QueryBuilder\AstToSql;
use Flow\PostgreSql\QueryBuilder\QualifiedIdentifier;

use function array_values;

final readonly class DropTypeBuilder implements DropTypeFinalStep
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

    public static function create(string ...$names): DropTypeFinalStep
    {
        return new self(array_values($names));
    }

    public function cascade(): DropTypeFinalStep
    {
        return new self($this->names, $this->ifExists, DropBehavior::DROP_CASCADE);
    }

    public function ifExists(): DropTypeFinalStep
    {
        return new self($this->names, true, $this->behavior);
    }

    public function restrict(): DropTypeFinalStep
    {
        return new self($this->names, $this->ifExists, DropBehavior::DROP_RESTRICT);
    }

    public function toAst(): DropStmt
    {
        $stmt = new DropStmt();
        $stmt->setRemoveType(ObjectType::OBJECT_TYPE);
        $stmt->setBehavior($this->behavior);
        $stmt->setMissingOk($this->ifExists);

        $objects = [];

        foreach ($this->names as $name) {
            $identifier = QualifiedIdentifier::parse($name);
            $nameNodes = [];

            foreach ($identifier->parts() as $part) {
                $str = new PBString();
                $str->setSval($part);
                $node = new Node();
                $node->setString($str);
                $nameNodes[] = $node;
            }

            $typeName = new TypeName();
            $typeName->setNames($nameNodes);
            $typeName->setTypemod(-1);

            $node = new Node();
            $node->setTypeName($typeName);
            $objects[] = $node;
        }

        $stmt->setObjects($objects);

        return $stmt;
    }
}
