<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Function;

use Flow\PostgreSql\Protobuf\AST\DropBehavior;
use Flow\PostgreSql\Protobuf\AST\DropStmt;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\ObjectType;
use Flow\PostgreSql\Protobuf\AST\ObjectWithArgs;
use Flow\PostgreSql\Protobuf\AST\PBString;
use Flow\PostgreSql\QueryBuilder\AstDeparser;
use Flow\PostgreSql\QueryBuilder\QualifiedIdentifier;

use function array_values;

final readonly class DropProcedureBuilder implements DropProcedureFinalStep
{
    /**
     * @param list<FunctionArgument> $arguments
     */
    private function __construct(
        private string $name,
        private ?string $schema = null,
        private array $arguments = [],
        private bool $ifExists = false,
        private int $behavior = DropBehavior::DROP_BEHAVIOR_UNDEFINED,
    ) {}

    public static function create(string $name): DropProcedureFinalStep
    {
        $identifier = QualifiedIdentifier::parse($name);

        return new self($identifier->name(), $identifier->schema());
    }

    public function arguments(FunctionArgument ...$args): DropProcedureFinalStep
    {
        return new self($this->name, $this->schema, array_values($args), $this->ifExists, $this->behavior);
    }

    public function cascade(): DropProcedureFinalStep
    {
        return new self($this->name, $this->schema, $this->arguments, $this->ifExists, DropBehavior::DROP_CASCADE);
    }

    public function ifExists(): DropProcedureFinalStep
    {
        return new self($this->name, $this->schema, $this->arguments, true, $this->behavior);
    }

    public function restrict(): DropProcedureFinalStep
    {
        return new self($this->name, $this->schema, $this->arguments, $this->ifExists, DropBehavior::DROP_RESTRICT);
    }

    public function toSql(): string
    {
        return (new AstDeparser())->deparse($this->toAst());
    }

    public function toAst(): DropStmt
    {
        $stmt = new DropStmt();
        $stmt->setRemoveType(ObjectType::OBJECT_PROCEDURE);

        if ($this->ifExists) {
            $stmt->setMissingOk(true);
        }

        if ($this->behavior !== DropBehavior::DROP_BEHAVIOR_UNDEFINED) {
            $stmt->setBehavior($this->behavior);
        }

        $objectWithArgs = new ObjectWithArgs();

        $objnameNodes = [];

        if ($this->schema !== null) {
            $schemaStr = new PBString();
            $schemaStr->setSval($this->schema);
            $schemaNode = new Node();
            $schemaNode->setString($schemaStr);
            $objnameNodes[] = $schemaNode;
        }

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
