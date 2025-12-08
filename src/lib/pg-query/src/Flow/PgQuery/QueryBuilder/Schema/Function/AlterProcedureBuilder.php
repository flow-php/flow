<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Function;

use Flow\PgQuery\Protobuf\AST\{A_Const, AlterFunctionStmt, DefElem, Integer, Node, ObjectType, ObjectWithArgs, PBString, RenameStmt, TypeName};

final readonly class AlterProcedureBuilder implements AlterProcedureArgsStep, AlterProcedureFinalStep
{
    /**
     * @param list<FunctionArgument> $arguments
     * @param list<array{name: string, arg: ?Node}> $actions
     */
    private function __construct(
        private string $name,
        private array $arguments = [],
        private array $actions = [],
        private ?string $renameTo = null,
    ) {
    }

    public static function create(string $name) : AlterProcedureArgsStep
    {
        return new self($name);
    }

    public function arguments(FunctionArgument ...$args) : AlterProcedureFinalStep
    {
        return new self(
            $this->name,
            \array_values($args),
            $this->actions,
            $this->renameTo,
        );
    }

    public function renameTo(string $newName) : AlterProcedureFinalStep
    {
        return new self(
            $this->name,
            $this->arguments,
            $this->actions,
            $newName,
        );
    }

    public function reset(string $parameter) : AlterProcedureFinalStep
    {
        $defElem = new DefElem();
        $defElem->setDefname($parameter);

        $node = new Node();
        $node->setDefElem($defElem);

        return $this->withAction('reset', $node);
    }

    public function resetAll() : AlterProcedureFinalStep
    {
        return $this->withAction('resetall', null);
    }

    public function securityDefiner() : AlterProcedureFinalStep
    {
        $integer = new Integer();
        $integer->setIval(1);

        $aConst = new A_Const();
        /** @phpstan-ignore argument.type (protobuf PHPDoc says int but actually expects Integer) */
        $aConst->setIval($integer);

        $defElem = new DefElem();
        $defElem->setDefname('security_definer');

        $argNode = new Node();
        $argNode->setAConst($aConst);
        $defElem->setArg($argNode);

        $node = new Node();
        $node->setDefElem($defElem);

        return $this->withAction('security_definer', $node);
    }

    public function securityInvoker() : AlterProcedureFinalStep
    {
        $integer = new Integer();
        $integer->setIval(0);

        $aConst = new A_Const();
        /** @phpstan-ignore argument.type (protobuf PHPDoc says int but actually expects Integer) */
        $aConst->setIval($integer);

        $defElem = new DefElem();
        $defElem->setDefname('security_definer');

        $argNode = new Node();
        $argNode->setAConst($aConst);
        $defElem->setArg($argNode);

        $node = new Node();
        $node->setDefElem($defElem);

        return $this->withAction('security_definer', $node);
    }

    public function set(string $parameter, string $value) : AlterProcedureFinalStep
    {
        $defElem = new DefElem();
        $defElem->setDefname($parameter);

        $str = new PBString();
        $str->setSval($value);

        $aConst = new A_Const();
        $aConst->setSval($str);

        $argNode = new Node();
        $argNode->setAConst($aConst);

        $defElem->setArg($argNode);

        $node = new Node();
        $node->setDefElem($defElem);

        return $this->withAction('set', $node);
    }

    public function toAlterAst() : AlterFunctionStmt
    {
        $stmt = new AlterFunctionStmt();
        $stmt->setObjtype(ObjectType::OBJECT_PROCEDURE);

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

        $stmt->setFunc($objectWithArgs);

        $actionNodes = [];

        foreach ($this->actions as $action) {
            if ($action['arg'] !== null) {
                $actionNodes[] = $action['arg'];
            } else {
                $defElem = new DefElem();
                $defElem->setDefname($action['name']);

                $node = new Node();
                $node->setDefElem($defElem);
                $actionNodes[] = $node;
            }
        }

        if ($actionNodes !== []) {
            $stmt->setActions($actionNodes);
        }

        return $stmt;
    }

    public function toRenameAst() : RenameStmt
    {
        $stmt = new RenameStmt();
        $stmt->setRenameType(ObjectType::OBJECT_PROCEDURE);

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
        $stmt->setObject($objectNode);

        if ($this->renameTo !== null) {
            $stmt->setNewname($this->renameTo);
        }

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

    private function withAction(string $name, ?Node $arg) : self
    {
        $newActions = $this->actions;
        $newActions[] = ['name' => $name, 'arg' => $arg];

        return new self(
            $this->name,
            $this->arguments,
            $newActions,
            $this->renameTo,
        );
    }
}
