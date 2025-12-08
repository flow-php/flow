<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Function;

use Flow\PgQuery\Protobuf\AST\{DropBehavior, DropStmt, Node, ObjectType, ObjectWithArgs, PBString, TypeName};

final readonly class DropFunctionBuilder implements DropFunctionFinalStep
{
    /**
     * @param list<FunctionArgument> $arguments
     */
    private function __construct(
        private string $name,
        private array $arguments = [],
        private bool $ifExists = false,
        private int $behavior = DropBehavior::DROP_BEHAVIOR_UNDEFINED,
    ) {
    }

    public static function create(string $name) : DropFunctionFinalStep
    {
        return new self($name);
    }

    public function arguments(FunctionArgument ...$args) : DropFunctionFinalStep
    {
        return new self(
            $this->name,
            \array_values($args),
            $this->ifExists,
            $this->behavior,
        );
    }

    public function cascade() : DropFunctionFinalStep
    {
        return new self(
            $this->name,
            $this->arguments,
            $this->ifExists,
            DropBehavior::DROP_CASCADE,
        );
    }

    public function ifExists() : DropFunctionFinalStep
    {
        return new self(
            $this->name,
            $this->arguments,
            true,
            $this->behavior,
        );
    }

    public function restrict() : DropFunctionFinalStep
    {
        return new self(
            $this->name,
            $this->arguments,
            $this->ifExists,
            DropBehavior::DROP_RESTRICT,
        );
    }

    public function toAst() : DropStmt
    {
        $stmt = new DropStmt();
        $stmt->setRemoveType(ObjectType::OBJECT_FUNCTION);

        if ($this->ifExists) {
            $stmt->setMissingOk(true);
        }

        if ($this->behavior !== DropBehavior::DROP_BEHAVIOR_UNDEFINED) {
            $stmt->setBehavior($this->behavior);
        }

        $objectWithArgs = new ObjectWithArgs();

        $objnameNodes = [];
        $str = new PBString();
        $str->setSval($this->name);
        $node = new Node();
        $node->setString($str);
        $objnameNodes[] = $node;
        $objectWithArgs->setObjname($objnameNodes);

        if ($this->arguments !== []) {
            $argNodes = [];

            foreach ($this->arguments as $arg) {
                $typeName = $this->createTypeName($arg->type);
                $node = new Node();
                $node->setTypeName($typeName);
                $argNodes[] = $node;
            }

            $objectWithArgs->setObjargs($argNodes);
        } else {
            $objectWithArgs->setArgsUnspecified(true);
        }

        $objectNode = new Node();
        $objectNode->setObjectWithArgs($objectWithArgs);

        $stmt->setObjects([$objectNode]);

        return $stmt;
    }

    private function createTypeName(string $type) : TypeName
    {
        $typeName = new TypeName();

        $typeNames = [];
        $str = new PBString();
        $str->setSval($type);
        $node = new Node();
        $node->setString($str);
        $typeNames[] = $node;
        $typeName->setNames($typeNames);

        return $typeName;
    }
}
