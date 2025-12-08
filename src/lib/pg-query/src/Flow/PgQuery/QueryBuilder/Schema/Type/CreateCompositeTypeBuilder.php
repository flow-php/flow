<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Type;

use Flow\PgQuery\Protobuf\AST\{CollateClause, ColumnDef, CompositeTypeStmt, Node, PBString, RangeVar, TypeName};

final readonly class CreateCompositeTypeBuilder implements CreateCompositeTypeAttributesStep, CreateCompositeTypeFinalStep
{
    /**
     * @param list<TypeAttribute> $attributes
     */
    private function __construct(
        private string $name,
        private ?string $schema = null,
        private array $attributes = [],
    ) {
    }

    public static function create(string $name) : CreateCompositeTypeAttributesStep
    {
        $parts = \explode('.', $name);

        if (\count($parts) === 2) {
            return new self($parts[1], $parts[0]);
        }

        return new self($name);
    }

    public function attributes(TypeAttribute ...$attributes) : CreateCompositeTypeFinalStep
    {
        return new self(
            $this->name,
            $this->schema,
            \array_values($attributes),
        );
    }

    public function toAst() : CompositeTypeStmt
    {
        $stmt = new CompositeTypeStmt();

        $typevar = new RangeVar();
        $typevar->setRelname($this->name);
        $typevar->setRelpersistence('p');
        $typevar->setInh(true);

        if ($this->schema !== null) {
            $typevar->setSchemaname($this->schema);
        }

        $stmt->setTypevar($typevar);

        $coldefNodes = [];

        foreach ($this->attributes as $attr) {
            $coldef = new ColumnDef();
            $coldef->setColname($attr->name);
            $coldef->setTypeName($this->createTypeName($attr->type));

            if ($attr->collation !== null) {
                $collClause = new CollateClause();

                $collNameNodes = [];
                $str = new PBString();
                $str->setSval($attr->collation);
                $node = new Node();
                $node->setString($str);
                $collNameNodes[] = $node;

                $collClause->setCollname($collNameNodes);
                $coldef->setCollClause($collClause);
            }

            $node = new Node();
            $node->setColumnDef($coldef);
            $coldefNodes[] = $node;
        }

        $stmt->setColdeflist($coldefNodes);

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
}
