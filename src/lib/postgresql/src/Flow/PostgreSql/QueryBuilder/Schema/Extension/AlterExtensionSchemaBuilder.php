<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Extension;

use Flow\PostgreSql\Protobuf\AST\AlterObjectSchemaStmt;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\ObjectType;
use Flow\PostgreSql\Protobuf\AST\PBString;
use Flow\PostgreSql\QueryBuilder\AstDeparser;

final readonly class AlterExtensionSchemaBuilder implements AlterExtensionSchemaFinalStep
{
    private function __construct(
        private string $name,
        private string $newSchema,
    ) {}

    public static function create(string $name, string $newSchema): self
    {
        return new self($name, $newSchema);
    }

    public function toSql(): string
    {
        return (new AstDeparser())->deparse($this->toAst());
    }

    public function toAst(): AlterObjectSchemaStmt
    {
        $name = new PBString();
        $name->setSval($this->name);

        $object = new Node();
        $object->setString($name);

        $stmt = new AlterObjectSchemaStmt();
        $stmt->setObjectType(ObjectType::OBJECT_EXTENSION);
        $stmt->setObject($object);
        $stmt->setNewschema($this->newSchema);

        return $stmt;
    }
}
