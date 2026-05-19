<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Function;

use Flow\PostgreSql\Protobuf\AST\A_Const;
use Flow\PostgreSql\Protobuf\AST\AlterFunctionStmt;
use Flow\PostgreSql\Protobuf\AST\DefElem;
use Flow\PostgreSql\Protobuf\AST\Integer;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\ObjectType;
use Flow\PostgreSql\Protobuf\AST\ObjectWithArgs;
use Flow\PostgreSql\Protobuf\AST\PBString;
use Flow\PostgreSql\Protobuf\AST\RenameStmt;
use Flow\PostgreSql\QueryBuilder\AstToSql;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidBuilderStateException;

use function array_values;

final readonly class AlterProcedureBuilder implements AlterProcedureArgsStep, AlterProcedureFinalStep
{
    use AstToSql;

    /**
     * @param list<FunctionArgument> $arguments
     * @param list<array{name: string, arg: ?Node}> $actions
     */
    private function __construct(
        private string $name,
        private array $arguments = [],
        private array $actions = [],
        private ?string $renameTo = null,
    ) {}

    public static function create(string $name): AlterProcedureArgsStep
    {
        return new self($name);
    }

    public function arguments(FunctionArgument ...$args): AlterProcedureFinalStep
    {
        return new self($this->name, array_values($args), $this->actions, $this->renameTo);
    }

    public function renameTo(string $newName): AlterProcedureFinalStep
    {
        return new self($this->name, $this->arguments, $this->actions, $newName);
    }

    public function reset(string $parameter): AlterProcedureFinalStep
    {
        $defElem = new DefElem();
        $defElem->setDefname($parameter);

        $node = new Node();
        $node->setDefElem($defElem);

        return $this->withAction('reset', $node);
    }

    public function resetAll(): AlterProcedureFinalStep
    {
        return $this->withAction('resetall', null);
    }

    public function securityDefiner(): AlterProcedureFinalStep
    {
        $integer = new Integer();
        $integer->setIval(1);

        $aConst = new A_Const(['ival' => $integer]);

        $defElem = new DefElem();
        $defElem->setDefname('security_definer');

        $argNode = new Node();
        $argNode->setAConst($aConst);
        $defElem->setArg($argNode);

        $node = new Node();
        $node->setDefElem($defElem);

        return $this->withAction('security_definer', $node);
    }

    public function securityInvoker(): AlterProcedureFinalStep
    {
        $integer = new Integer();
        $integer->setIval(0);

        $aConst = new A_Const(['ival' => $integer]);

        $defElem = new DefElem();
        $defElem->setDefname('security_definer');

        $argNode = new Node();
        $argNode->setAConst($aConst);
        $defElem->setArg($argNode);

        $node = new Node();
        $node->setDefElem($defElem);

        return $this->withAction('security_definer', $node);
    }

    public function set(string $parameter, string $value): AlterProcedureFinalStep
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

    public function toAst(): AlterFunctionStmt|RenameStmt
    {
        if ($this->renameTo !== null && $this->actions !== []) {
            throw InvalidBuilderStateException::mutuallyExclusiveOptions('RENAME TO', 'other alterations');
        }

        if ($this->renameTo !== null) {
            return $this->buildRenameAst();
        }

        return $this->buildAlterAst();
    }

    private function buildAlterAst(): AlterFunctionStmt
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
                $node = new Node();
                $node->setTypeName($arg->type->toAst());
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

    private function buildRenameAst(): RenameStmt
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
        $stmt->setObject($objectNode);

        if ($this->renameTo !== null) {
            $stmt->setNewname($this->renameTo);
        }

        return $stmt;
    }

    private function withAction(string $name, ?Node $arg): self
    {
        $newActions = $this->actions;
        $newActions[] = ['name' => $name, 'arg' => $arg];

        return new self($this->name, $this->arguments, $newActions, $this->renameTo);
    }
}
