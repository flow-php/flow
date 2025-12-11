<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Type;

use Flow\PgQuery\Protobuf\AST\{CreateRangeStmt, DefElem, Node, PBString, TypeName};
use Flow\PgQuery\QueryBuilder\{AstToSql, QualifiedIdentifier};

final readonly class CreateRangeTypeBuilder implements CreateRangeTypeOptionsStep, CreateRangeTypeSubtypeStep
{
    use AstToSql;

    /**
     * @param list<array{name: string, arg: Node}> $params
     */
    private function __construct(
        private string $name,
        private ?string $schema = null,
        private array $params = [],
    ) {
    }

    public static function create(string $name) : CreateRangeTypeSubtypeStep
    {
        $identifier = QualifiedIdentifier::parse($name);

        return new self($identifier->name(), $identifier->schema());
    }

    public function canonical(string $function) : CreateRangeTypeOptionsStep
    {
        return $this->withStringParam('canonical', $function);
    }

    public function collation(string $collation) : CreateRangeTypeOptionsStep
    {
        return $this->withStringParam('collation', $collation);
    }

    public function multirangeTypeName(string $name) : CreateRangeTypeOptionsStep
    {
        return $this->withStringParam('multirange_type_name', $name);
    }

    public function subtype(string $type) : CreateRangeTypeOptionsStep
    {
        return $this->withTypeNameParam('subtype', $type);
    }

    public function subtypeDiff(string $function) : CreateRangeTypeOptionsStep
    {
        return $this->withStringParam('subtype_diff', $function);
    }

    public function subtypeOpclass(string $opclass) : CreateRangeTypeOptionsStep
    {
        return $this->withStringParam('subtype_opclass', $opclass);
    }

    public function toAst() : CreateRangeStmt
    {
        $stmt = new CreateRangeStmt();

        $typeNameNodes = [];

        if ($this->schema !== null) {
            $str = new PBString();
            $str->setSval($this->schema);
            $node = new Node();
            $node->setString($str);
            $typeNameNodes[] = $node;
        }

        $str = new PBString();
        $str->setSval($this->name);
        $node = new Node();
        $node->setString($str);
        $typeNameNodes[] = $node;

        $stmt->setTypeName($typeNameNodes);

        $paramNodes = [];

        foreach ($this->params as $param) {
            $defElem = new DefElem();
            $defElem->setDefname($param['name']);
            $defElem->setArg($param['arg']);

            $node = new Node();
            $node->setDefElem($defElem);
            $paramNodes[] = $node;
        }

        if ($paramNodes !== []) {
            $stmt->setParams($paramNodes);
        }

        return $stmt;
    }

    private function withParam(string $name, Node $arg) : self
    {
        $newParams = $this->params;
        $newParams[] = ['name' => $name, 'arg' => $arg];

        return new self(
            $this->name,
            $this->schema,
            $newParams,
        );
    }

    private function withStringParam(string $name, string $value) : self
    {
        $str = new PBString();
        $str->setSval($value);

        $argNode = new Node();
        $argNode->setString($str);

        return $this->withParam($name, $argNode);
    }

    private function withTypeNameParam(string $name, string $type) : self
    {
        $typeName = new TypeName();

        $typeNames = [];
        $str = new PBString();
        $str->setSval($type);
        $node = new Node();
        $node->setString($str);
        $typeNames[] = $node;
        $typeName->setNames($typeNames);

        $argNode = new Node();
        $argNode->setTypeName($typeName);

        return $this->withParam($name, $argNode);
    }
}
