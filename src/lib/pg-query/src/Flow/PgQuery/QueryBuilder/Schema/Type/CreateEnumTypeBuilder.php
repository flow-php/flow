<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Type;

use Flow\PgQuery\Protobuf\AST\{CreateEnumStmt, Node, PBString};

final readonly class CreateEnumTypeBuilder implements CreateEnumTypeFinalStep, CreateEnumTypeLabelsStep
{
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
        $parts = \explode('.', $name);

        if (\count($parts) === 2) {
            return new self($parts[1], $parts[0]);
        }

        return new self($name);
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
