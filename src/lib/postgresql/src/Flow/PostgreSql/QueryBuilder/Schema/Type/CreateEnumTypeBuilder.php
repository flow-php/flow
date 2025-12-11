<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Type;

use Flow\PostgreSql\Protobuf\AST\{CreateEnumStmt, Node, PBString};
use Flow\PostgreSql\QueryBuilder\{AstToSql, QualifiedIdentifier};

final readonly class CreateEnumTypeBuilder implements CreateEnumTypeFinalStep, CreateEnumTypeLabelsStep
{
    use AstToSql;

    /**
     * @param list<string> $labels
     */
    private function __construct(
        private string $name,
        private ?string $schema = null,
        private array $labels = [],
    ) {
    }

    public static function create(string $name) : CreateEnumTypeLabelsStep
    {
        $identifier = QualifiedIdentifier::parse($name);

        return new self($identifier->name(), $identifier->schema());
    }

    public function labels(string ...$labels) : CreateEnumTypeFinalStep
    {
        return new self(
            $this->name,
            $this->schema,
            \array_values($labels),
        );
    }

    public function toAst() : CreateEnumStmt
    {
        $stmt = new CreateEnumStmt();

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

        $valNodes = [];

        foreach ($this->labels as $label) {
            $str = new PBString();
            $str->setSval($label);
            $node = new Node();
            $node->setString($str);
            $valNodes[] = $node;
        }

        $stmt->setVals($valNodes);

        return $stmt;
    }
}
