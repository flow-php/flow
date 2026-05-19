<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Function;

use Flow\PostgreSql\Protobuf\AST\A_Const;
use Flow\PostgreSql\Protobuf\AST\CreateFunctionStmt;
use Flow\PostgreSql\Protobuf\AST\DefElem;
use Flow\PostgreSql\Protobuf\AST\FunctionParameter;
use Flow\PostgreSql\Protobuf\AST\Integer;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\PBList;
use Flow\PostgreSql\Protobuf\AST\PBString;
use Flow\PostgreSql\QueryBuilder\AstToSql;
use Flow\PostgreSql\QueryBuilder\QualifiedIdentifier;

use function array_values;

final readonly class CreateProcedureBuilder implements
    CreateProcedureArgsStep,
    CreateProcedureFinalStep,
    CreateProcedureOptionsStep
{
    use AstToSql;

    /**
     * @param list<FunctionArgument> $arguments
     * @param list<array{name: string, arg: ?Node}> $options
     */
    private function __construct(
        private string $name,
        private ?string $schema = null,
        private bool $replace = false,
        private array $arguments = [],
        private array $options = [],
    ) {}

    public static function create(string $name): CreateProcedureArgsStep
    {
        $identifier = QualifiedIdentifier::parse($name);

        return new self($identifier->name(), $identifier->schema());
    }

    public function arguments(FunctionArgument ...$args): CreateProcedureOptionsStep
    {
        return new self($this->name, $this->schema, $this->replace, array_values($args), $this->options);
    }

    public function as(string $definition): CreateProcedureFinalStep
    {
        return $this->withListOption('as', $definition);
    }

    public function language(string $language): CreateProcedureOptionsStep
    {
        return $this->withStringOption('language', $language);
    }

    public function orReplace(): CreateProcedureArgsStep
    {
        return new self($this->name, $this->schema, true, $this->arguments, $this->options);
    }

    public function securityDefiner(): CreateProcedureOptionsStep
    {
        $integer = new Integer();
        $integer->setIval(1);

        $argNode = new Node(['integer' => $integer]);

        return $this->withOption('security_definer', $argNode);
    }

    public function securityInvoker(): CreateProcedureOptionsStep
    {
        $integer = new Integer();
        $integer->setIval(0);

        $argNode = new Node(['integer' => $integer]);

        return $this->withOption('security_definer', $argNode);
    }

    public function set(string $parameter, string $value): CreateProcedureOptionsStep
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

        $setNode = new Node();
        $setNode->setDefElem($defElem);

        $newOptions = $this->options;
        $newOptions[] = ['name' => 'set', 'arg' => $setNode];

        return new self($this->name, $this->schema, $this->replace, $this->arguments, $newOptions);
    }

    public function toAst(): CreateFunctionStmt
    {
        $stmt = new CreateFunctionStmt();
        $stmt->setIsProcedure(true);
        $stmt->setReplace($this->replace);

        $funcnameNodes = [];

        if ($this->schema !== null) {
            $str = new PBString();
            $str->setSval($this->schema);
            $node = new Node();
            $node->setString($str);
            $funcnameNodes[] = $node;
        }

        $str = new PBString();
        $str->setSval($this->name);
        $node = new Node();
        $node->setString($str);
        $funcnameNodes[] = $node;
        $stmt->setFuncname($funcnameNodes);

        if ($this->arguments !== []) {
            $parameterNodes = [];

            foreach ($this->arguments as $arg) {
                $param = new FunctionParameter();

                if ($arg->name !== null) {
                    $param->setName($arg->name);
                }

                $param->setMode($arg->mode->value);
                $param->setArgType($arg->type->toAst());

                if ($arg->default !== null) {
                    $param->setDefexpr($this->createDefaultExpr($arg->default));
                }

                $node = new Node();
                $node->setFunctionParameter($param);
                $parameterNodes[] = $node;
            }

            $stmt->setParameters($parameterNodes);
        }

        $optionNodes = [];

        foreach ($this->options as $option) {
            if ($option['name'] === 'set' && $option['arg'] !== null) {
                $optionNodes[] = $option['arg'];
            } else {
                $defElem = new DefElem();
                $defElem->setDefname($option['name']);

                if ($option['arg'] !== null) {
                    $defElem->setArg($option['arg']);
                }

                $node = new Node();
                $node->setDefElem($defElem);
                $optionNodes[] = $node;
            }
        }

        if ($optionNodes !== []) {
            $stmt->setOptions($optionNodes);
        }

        return $stmt;
    }

    private function createDefaultExpr(string $default): Node
    {
        $str = new PBString();
        $str->setSval($default);

        $aConst = new A_Const();
        $aConst->setSval($str);

        $node = new Node();
        $node->setAConst($aConst);

        return $node;
    }

    private function withListOption(string $name, string $value): self
    {
        $str = new PBString();
        $str->setSval($value);

        $itemNode = new Node();
        $itemNode->setString($str);

        $list = new PBList();
        $list->setItems([$itemNode]);

        $argNode = new Node();
        $argNode->setList($list);

        return $this->withOption($name, $argNode);
    }

    private function withOption(string $name, ?Node $arg): self
    {
        $newOptions = $this->options;
        $newOptions[] = ['name' => $name, 'arg' => $arg];

        return new self($this->name, $this->schema, $this->replace, $this->arguments, $newOptions);
    }

    private function withStringOption(string $name, string $value): self
    {
        $str = new PBString();
        $str->setSval($value);

        $argNode = new Node();
        $argNode->setString($str);

        return $this->withOption($name, $argNode);
    }
}
