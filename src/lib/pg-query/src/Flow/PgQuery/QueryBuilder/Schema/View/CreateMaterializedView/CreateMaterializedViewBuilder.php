<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\View\CreateMaterializedView;

use Flow\PgQuery\Protobuf\AST\{CreateTableAsStmt, IntoClause, Node, ObjectType, PBString, RangeVar};
use Flow\PgQuery\QueryBuilder\{AstToSql, QualifiedIdentifier};
use Flow\PgQuery\QueryBuilder\Exception\InvalidExpressionException;
use Flow\PgQuery\QueryBuilder\Select\SelectFinalStep;

final readonly class CreateMaterializedViewBuilder implements CreateMatViewAsStep, CreateMatViewDataStep, CreateMatViewFinalStep, CreateMatViewOptionsStep
{
    use AstToSql;

    /**
     * @param list<string> $columns
     */
    private function __construct(
        private ?string $name = null,
        private ?string $schema = null,
        private array $columns = [],
        private ?SelectFinalStep $query = null,
        private bool $ifNotExists = false,
        private ?string $accessMethod = null,
        private ?string $tablespace = null,
        private ?bool $skipData = null,
    ) {
    }

    public static function create(string $name, ?string $schema = null) : CreateMatViewOptionsStep
    {
        if ($schema !== null) {
            return new self($name, $schema);
        }

        $identifier = QualifiedIdentifier::parse($name);

        return new self($identifier->name(), $identifier->schema());
    }

    public function as(SelectFinalStep $query) : CreateMatViewDataStep
    {
        return new self(
            $this->name,
            $this->schema,
            $this->columns,
            $query,
            $this->ifNotExists,
            $this->accessMethod,
            $this->tablespace,
            $this->skipData,
        );
    }

    public function columns(string ...$columns) : CreateMatViewAsStep
    {
        return new self(
            $this->name,
            $this->schema,
            \array_values($columns),
            $this->query,
            $this->ifNotExists,
            $this->accessMethod,
            $this->tablespace,
            $this->skipData,
        );
    }

    public function ifNotExists() : CreateMatViewOptionsStep
    {
        return new self(
            $this->name,
            $this->schema,
            $this->columns,
            $this->query,
            true,
            $this->accessMethod,
            $this->tablespace,
            $this->skipData,
        );
    }

    public function tablespace(string $tablespace) : CreateMatViewDataStep
    {
        return new self(
            $this->name,
            $this->schema,
            $this->columns,
            $this->query,
            $this->ifNotExists,
            $this->accessMethod,
            $tablespace,
            $this->skipData,
        );
    }

    public function toAst() : CreateTableAsStmt
    {
        if ($this->name === null || $this->name === '') {
            throw InvalidExpressionException::invalidValue('materialized view name', 'null or empty');
        }

        if ($this->query === null) {
            throw InvalidExpressionException::invalidValue('query', 'null');
        }

        $stmt = new CreateTableAsStmt();

        $stmt->setObjtype(ObjectType::OBJECT_MATVIEW);

        $into = new IntoClause();

        $rangeVar = new RangeVar();
        $rangeVar->setRelname($this->name);
        $rangeVar->setRelpersistence('p');
        $rangeVar->setInh(true);

        if ($this->schema !== null) {
            $rangeVar->setSchemaname($this->schema);
        }

        $into->setRel($rangeVar);

        if ($this->columns !== []) {
            $colNames = [];

            foreach ($this->columns as $column) {
                $str = new PBString();
                $str->setSval($column);
                $node = new Node();
                $node->setString($str);
                $colNames[] = $node;
            }

            $into->setColNames($colNames);
        }

        if ($this->accessMethod !== null) {
            $into->setAccessMethod($this->accessMethod);
        }

        if ($this->tablespace !== null) {
            $into->setTableSpaceName($this->tablespace);
        }

        if ($this->skipData !== null) {
            $into->setSkipData($this->skipData);
        }

        $stmt->setInto($into);

        $queryNode = new Node();
        $queryNode->setSelectStmt($this->query->toAst());
        $stmt->setQuery($queryNode);

        if ($this->ifNotExists) {
            $stmt->setIfNotExists(true);
        }

        return $stmt;
    }

    public function using(string $method) : CreateMatViewOptionsStep
    {
        return new self(
            $this->name,
            $this->schema,
            $this->columns,
            $this->query,
            $this->ifNotExists,
            $method,
            $this->tablespace,
            $this->skipData,
        );
    }

    public function withData() : CreateMatViewFinalStep
    {
        return new self(
            $this->name,
            $this->schema,
            $this->columns,
            $this->query,
            $this->ifNotExists,
            $this->accessMethod,
            $this->tablespace,
            false,
        );
    }

    public function withNoData() : CreateMatViewFinalStep
    {
        return new self(
            $this->name,
            $this->schema,
            $this->columns,
            $this->query,
            $this->ifNotExists,
            $this->accessMethod,
            $this->tablespace,
            true,
        );
    }
}
