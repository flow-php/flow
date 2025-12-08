<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Function;

use Flow\PgQuery\Protobuf\AST\{A_Const, CreateFunctionStmt, DefElem, FunctionParameter, FunctionParameterMode, Integer, Node, PBList, PBString, TypeName};

final readonly class CreateFunctionBuilder implements CreateFunctionArgsStep, CreateFunctionFinalStep, CreateFunctionOptionsStep, CreateFunctionReturnsStep
{
    /**
     * @param list<FunctionArgument> $arguments
     * @param list<array{name: string, arg: ?Node}> $options
     * @param null|array<string, string> $tableColumns
     */
    private function __construct(
        private string $name,
        private bool $replace = false,
        private array $arguments = [],
        private ?string $returnType = null,
        private bool $setof = false,
        private ?array $tableColumns = null,
        private array $options = [],
    ) {
    }

    public static function create(string $name) : CreateFunctionArgsStep
    {
        return new self($name);
    }

    public function arguments(FunctionArgument ...$args) : CreateFunctionReturnsStep
    {
        return new self(
            $this->name,
            $this->replace,
            \array_values($args),
            $this->returnType,
            $this->setof,
            $this->tableColumns,
            $this->options,
        );
    }

    public function as(string $definition) : CreateFunctionFinalStep
    {
        return $this->withListOption('as', $definition);
    }

    public function calledOnNullInput() : CreateFunctionOptionsStep
    {
        return $this->withBooleanOption('strict', false);
    }

    public function cost(int $cost) : CreateFunctionOptionsStep
    {
        return $this->withIntegerOption('cost', $cost);
    }

    public function immutable() : CreateFunctionOptionsStep
    {
        return $this->withStringOption('volatility', 'immutable');
    }

    public function language(string $language) : CreateFunctionOptionsStep
    {
        return $this->withStringOption('language', $language);
    }

    public function leakproof(bool $value = true) : CreateFunctionOptionsStep
    {
        return $this->withBooleanOption('leakproof', $value);
    }

    public function orReplace() : CreateFunctionArgsStep
    {
        return new self(
            $this->name,
            true,
            $this->arguments,
            $this->returnType,
            $this->setof,
            $this->tableColumns,
            $this->options,
        );
    }

    public function parallel(ParallelSafety $safety) : CreateFunctionOptionsStep
    {
        return $this->withStringOption('parallel', $safety->value);
    }

    public function returns(string $type) : CreateFunctionOptionsStep
    {
        return new self(
            $this->name,
            $this->replace,
            $this->arguments,
            $type,
            false,
            null,
            $this->options,
        );
    }

    public function returnsSetOf(string $type) : CreateFunctionOptionsStep
    {
        return new self(
            $this->name,
            $this->replace,
            $this->arguments,
            $type,
            true,
            null,
            $this->options,
        );
    }

    /**
     * @param array<string, string> $columns
     */
    public function returnsTable(array $columns) : CreateFunctionOptionsStep
    {
        return new self(
            $this->name,
            $this->replace,
            $this->arguments,
            null,
            false,
            $columns,
            $this->options,
        );
    }

    public function returnsVoid() : CreateFunctionOptionsStep
    {
        return new self(
            $this->name,
            $this->replace,
            $this->arguments,
            'void',
            false,
            null,
            $this->options,
        );
    }

    public function rows(int $rows) : CreateFunctionOptionsStep
    {
        return $this->withIntegerOption('rows', $rows);
    }

    public function securityDefiner() : CreateFunctionOptionsStep
    {
        return $this->withBooleanOption('security_definer', true);
    }

    public function securityInvoker() : CreateFunctionOptionsStep
    {
        return $this->withBooleanOption('security_definer', false);
    }

    public function set(string $parameter, string $value) : CreateFunctionOptionsStep
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

        return new self(
            $this->name,
            $this->replace,
            $this->arguments,
            $this->returnType,
            $this->setof,
            $this->tableColumns,
            $newOptions,
        );
    }

    public function stable() : CreateFunctionOptionsStep
    {
        return $this->withStringOption('volatility', 'stable');
    }

    public function strict() : CreateFunctionOptionsStep
    {
        return $this->withBooleanOption('strict', true);
    }

    public function toAst() : CreateFunctionStmt
    {
        $stmt = new CreateFunctionStmt();
        $stmt->setIsProcedure(false);
        $stmt->setReplace($this->replace);

        $funcnameNodes = [];
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

                $typeName = $this->createTypeName($arg->type);
                $param->setArgType($typeName);

                if ($arg->default !== null) {
                    $param->setDefexpr($this->createDefaultExpr($arg->default));
                }

                $node = new Node();
                $node->setFunctionParameter($param);
                $parameterNodes[] = $node;
            }

            $stmt->setParameters($parameterNodes);
        }

        if ($this->returnType !== null) {
            $typeName = $this->createTypeName($this->returnType);

            if ($this->setof) {
                $typeName->setSetof(true);
            }

            $stmt->setReturnType($typeName);
        } elseif ($this->tableColumns !== null) {
            $parameterNodes = [];

            foreach ($this->tableColumns as $columnName => $columnType) {
                $param = new FunctionParameter();
                $param->setName($columnName);
                $param->setMode(FunctionParameterMode::FUNC_PARAM_TABLE);
                $param->setArgType($this->createTypeName($columnType));

                $node = new Node();
                $node->setFunctionParameter($param);
                $parameterNodes[] = $node;
            }

            $currentParams = [];

            foreach ($stmt->getParameters() as $existingParam) {
                $currentParams[] = $existingParam;
            }

            $stmt->setParameters(\array_merge($currentParams, $parameterNodes));
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

    public function volatile() : CreateFunctionOptionsStep
    {
        return $this->withStringOption('volatility', 'volatile');
    }

    private function createDefaultExpr(string $default) : Node
    {
        $str = new PBString();
        $str->setSval($default);

        $aConst = new A_Const();
        $aConst->setSval($str);

        $node = new Node();
        $node->setAConst($aConst);

        return $node;
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

    private function withBooleanOption(string $name, bool $value) : self
    {
        $integer = new Integer();
        $integer->setIval($value ? 1 : 0);

        $argNode = new Node();
        /** @phpstan-ignore argument.type (protobuf PHPDoc says int but actually expects Integer) */
        $argNode->setInteger($integer);

        return $this->withOption($name, $argNode);
    }

    private function withIntegerOption(string $name, int $value) : self
    {
        $integer = new Integer();
        $integer->setIval($value);

        $aConst = new A_Const();
        /** @phpstan-ignore argument.type (protobuf PHPDoc says int but actually expects Integer) */
        $aConst->setIval($integer);

        $argNode = new Node();
        $argNode->setAConst($aConst);

        return $this->withOption($name, $argNode);
    }

    private function withListOption(string $name, string $value) : self
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

    private function withOption(string $name, ?Node $arg) : self
    {
        $newOptions = $this->options;
        $newOptions[] = ['name' => $name, 'arg' => $arg];

        return new self(
            $this->name,
            $this->replace,
            $this->arguments,
            $this->returnType,
            $this->setof,
            $this->tableColumns,
            $newOptions,
        );
    }

    private function withStringOption(string $name, string $value) : self
    {
        $str = new PBString();
        $str->setSval($value);

        $argNode = new Node();
        $argNode->setString($str);

        return $this->withOption($name, $argNode);
    }
}
