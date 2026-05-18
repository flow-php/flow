<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Function;

use Flow\PostgreSql\Protobuf\AST\DropBehavior;
use Flow\PostgreSql\Protobuf\AST\DropStmt;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\ObjectType;
use Flow\PostgreSql\Protobuf\AST\ObjectWithArgs;
use Flow\PostgreSql\Protobuf\AST\PBString;
use Flow\PostgreSql\QueryBuilder\AstToSql;

use function array_values;

final readonly class DropFunctionBuilder implements DropFunctionFinalStep
{
    use AstToSql;

    /**
     * @param list<FunctionArgument> $arguments
     */
    private function __construct(
        private string $name,
        private array $arguments = [],
        private bool $ifExists = false,
        private int $behavior = DropBehavior::DROP_BEHAVIOR_UNDEFINED,
    ) {}

    public static function create(string $name): DropFunctionFinalStep
    {
        return new self($name);
    }

    public function arguments(FunctionArgument ...$args): DropFunctionFinalStep
    {
        return new self($this->name, array_values($args), $this->ifExists, $this->behavior);
    }

    public function cascade(): DropFunctionFinalStep
    {
        return new self($this->name, $this->arguments, $this->ifExists, DropBehavior::DROP_CASCADE);
    }

    public function ifExists(): DropFunctionFinalStep
    {
        return new self($this->name, $this->arguments, true, $this->behavior);
    }

    public function restrict(): DropFunctionFinalStep
    {
        return new self($this->name, $this->arguments, $this->ifExists, DropBehavior::DROP_RESTRICT);
    }

    public function toAst(): DropStmt
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
                $node = new Node();
                $node->setTypeName($arg->type->toAst());
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
}
