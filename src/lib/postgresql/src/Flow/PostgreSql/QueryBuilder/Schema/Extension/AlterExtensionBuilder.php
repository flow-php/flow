<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Extension;

use Flow\PostgreSql\Protobuf\AST\AlterExtensionContentsStmt;
use Flow\PostgreSql\Protobuf\AST\AlterExtensionStmt;
use Flow\PostgreSql\Protobuf\AST\DefElem;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\ObjectType;
use Flow\PostgreSql\Protobuf\AST\ObjectWithArgs;
use Flow\PostgreSql\Protobuf\AST\PBList;
use Flow\PostgreSql\Protobuf\AST\PBString;
use Flow\PostgreSql\QueryBuilder\AstToSql;
use Flow\PostgreSql\QueryBuilder\QualifiedIdentifier;

final readonly class AlterExtensionBuilder implements AlterExtensionActionStep, AlterExtensionFinalStep
{
    use AstToSql;

    private function __construct(
        private string $name,
        private bool $isUpdate = false,
        private ?string $version = null,
        private ?int $action = null,
        private ?int $objtype = null,
        private ?string $objectName = null,
    ) {}

    public static function create(string $name): AlterExtensionActionStep
    {
        return new self($name);
    }

    public function addFunction(string $function): AlterExtensionFinalStep
    {
        return new self($this->name, false, null, 1, ObjectType::OBJECT_FUNCTION, $function);
    }

    public function addTable(string $table): AlterExtensionFinalStep
    {
        return new self($this->name, false, null, 1, ObjectType::OBJECT_TABLE, $table);
    }

    public function dropFunction(string $function): AlterExtensionFinalStep
    {
        return new self($this->name, false, null, -1, ObjectType::OBJECT_FUNCTION, $function);
    }

    public function dropTable(string $table): AlterExtensionFinalStep
    {
        return new self($this->name, false, null, -1, ObjectType::OBJECT_TABLE, $table);
    }

    public function toAst(): AlterExtensionStmt|AlterExtensionContentsStmt
    {
        if ($this->isUpdate) {
            $stmt = new AlterExtensionStmt();
            $stmt->setExtname($this->name);

            if ($this->version !== null) {
                $defElem = new DefElem();
                $defElem->setDefname('new_version');

                $str = new PBString();
                $str->setSval($this->version);

                $argNode = new Node();
                $argNode->setString($str);

                $defElem->setArg($argNode);

                $node = new Node();
                $node->setDefElem($defElem);

                $stmt->setOptions([$node]);
            }

            return $stmt;
        }

        $stmt = new AlterExtensionContentsStmt();
        $stmt->setExtname($this->name);
        $stmt->setAction($this->action ?? 1);
        $stmt->setObjtype($this->objtype ?? ObjectType::OBJECT_TABLE);

        if ($this->objectName !== null) {
            $identifier = QualifiedIdentifier::parse($this->objectName);

            if ($this->objtype === ObjectType::OBJECT_FUNCTION) {
                $objectWithArgs = new ObjectWithArgs();

                $nameNodes = [];

                foreach ($identifier->parts() as $part) {
                    $str = new PBString();
                    $str->setSval($part);
                    $node = new Node();
                    $node->setString($str);
                    $nameNodes[] = $node;
                }

                $objectWithArgs->setObjname($nameNodes);
                $objectWithArgs->setArgsUnspecified(true);

                $objectNode = new Node();
                $objectNode->setObjectWithArgs($objectWithArgs);

                $stmt->setObject($objectNode);
            } else {
                $nameNodes = [];

                foreach ($identifier->parts() as $part) {
                    $str = new PBString();
                    $str->setSval($part);
                    $node = new Node();
                    $node->setString($str);
                    $nameNodes[] = $node;
                }

                $list = new PBList();
                $list->setItems($nameNodes);

                $objectNode = new Node();
                $objectNode->setList($list);

                $stmt->setObject($objectNode);
            }
        }

        return $stmt;
    }

    public function update(): AlterExtensionFinalStep
    {
        return new self($this->name, true, null, null, null, null);
    }

    public function updateTo(string $version): AlterExtensionFinalStep
    {
        return new self($this->name, true, $version, null, null, null);
    }
}
