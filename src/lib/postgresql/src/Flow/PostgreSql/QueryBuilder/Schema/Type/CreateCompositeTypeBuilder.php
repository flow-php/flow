<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Type;

use Flow\PostgreSql\Protobuf\AST\CollateClause;
use Flow\PostgreSql\Protobuf\AST\ColumnDef;
use Flow\PostgreSql\Protobuf\AST\CompositeTypeStmt;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\PBString;
use Flow\PostgreSql\Protobuf\AST\RangeVar;
use Flow\PostgreSql\QueryBuilder\AstToSql;
use Flow\PostgreSql\QueryBuilder\QualifiedIdentifier;

final readonly class CreateCompositeTypeBuilder implements
    CreateCompositeTypeAttributesStep,
    CreateCompositeTypeFinalStep
{
    use AstToSql;

    /**
     * @param list<TypeAttribute> $attributes
     */
    private function __construct(
        private string $name,
        private ?string $schema = null,
        private array $attributes = [],
    ) {}

    public static function create(string $name): CreateCompositeTypeAttributesStep
    {
        $identifier = QualifiedIdentifier::parse($name);

        return new self($identifier->name(), $identifier->schema());
    }

    public function attributes(TypeAttribute ...$attributes): CreateCompositeTypeFinalStep
    {
        return new self($this->name, $this->schema, \array_values($attributes));
    }

    public function toAst(): CompositeTypeStmt
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
            $coldef->setTypeName($attr->type->toAst());

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
}
