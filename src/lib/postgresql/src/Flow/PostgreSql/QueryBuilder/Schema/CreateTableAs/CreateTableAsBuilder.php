<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\CreateTableAs;

use Flow\PostgreSql\Protobuf\AST\CreateTableAsStmt;
use Flow\PostgreSql\Protobuf\AST\IntoClause;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\ObjectType;
use Flow\PostgreSql\Protobuf\AST\PBString;
use Flow\PostgreSql\Protobuf\AST\RangeVar;
use Flow\PostgreSql\QueryBuilder\AstToSql;
use Flow\PostgreSql\QueryBuilder\Select\SelectFinalStep;

final readonly class CreateTableAsBuilder implements CreateTableAsFinalStep
{
    use AstToSql;

    /**
     * @param list<string> $columnNames
     */
    private function __construct(
        private string $table,
        private ?string $schema,
        private SelectFinalStep $query,
        private array $columnNames = [],
        private bool $ifNotExists = false,
        private bool $withNoData = false,
    ) {}

    public static function create(string $table, SelectFinalStep $query, ?string $schema = null): CreateTableAsFinalStep
    {
        return new self($table, $schema, $query);
    }

    public function columnNames(string ...$names): CreateTableAsFinalStep
    {
        return new self(
            $this->table,
            $this->schema,
            $this->query,
            \array_values($names),
            $this->ifNotExists,
            $this->withNoData,
        );
    }

    public function ifNotExists(): CreateTableAsFinalStep
    {
        return new self($this->table, $this->schema, $this->query, $this->columnNames, true, $this->withNoData);
    }

    public function toAst(): CreateTableAsStmt
    {
        $stmt = new CreateTableAsStmt();

        $rangeVar = new RangeVar();
        $rangeVar->setRelname($this->table);
        $rangeVar->setRelpersistence('p');
        $rangeVar->setInh(true);

        if ($this->schema !== null) {
            $rangeVar->setSchemaname($this->schema);
        }

        $intoClause = new IntoClause();
        $intoClause->setRel($rangeVar);

        if ($this->columnNames !== []) {
            $colNames = [];

            foreach ($this->columnNames as $name) {
                $colNames[] = $this->createStringNode($name);
            }

            $intoClause->setColNames($colNames);
        }

        if ($this->withNoData) {
            $intoClause->setSkipData(true);
        }

        $stmt->setInto($intoClause);

        $queryNode = new Node();
        $queryNode->setSelectStmt($this->query->toAst());
        $stmt->setQuery($queryNode);

        $stmt->setObjtype(ObjectType::OBJECT_TABLE);

        if ($this->ifNotExists) {
            $stmt->setIfNotExists(true);
        }

        return $stmt;
    }

    public function withNoData(): CreateTableAsFinalStep
    {
        return new self($this->table, $this->schema, $this->query, $this->columnNames, $this->ifNotExists, true);
    }

    private function createStringNode(string $value): Node
    {
        $str = new PBString();
        $str->setSval($value);

        $node = new Node();
        $node->setString($str);

        return $node;
    }
}
