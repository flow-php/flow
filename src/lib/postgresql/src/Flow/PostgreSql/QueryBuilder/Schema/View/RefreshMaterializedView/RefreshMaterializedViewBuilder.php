<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\View\RefreshMaterializedView;

use Flow\PostgreSql\Protobuf\AST\{RangeVar, RefreshMatViewStmt};
use Flow\PostgreSql\QueryBuilder\{AstToSql, QualifiedIdentifier};
use Flow\PostgreSql\QueryBuilder\Exception\InvalidExpressionException;

final readonly class RefreshMaterializedViewBuilder implements RefreshMatViewFinalStep, RefreshMatViewOptionsStep
{
    use AstToSql;

    private function __construct(
        private ?string $name = null,
        private ?string $schema = null,
        private bool $concurrent = false,
        private ?bool $skipData = null,
    ) {
    }

    public static function create(string $name, ?string $schema = null) : RefreshMatViewOptionsStep
    {
        if ($schema !== null) {
            return new self($name, $schema);
        }

        $identifier = QualifiedIdentifier::parse($name);

        return new self($identifier->name(), $identifier->schema());
    }

    public function concurrently() : RefreshMatViewFinalStep
    {
        return new self(
            $this->name,
            $this->schema,
            true,
            $this->skipData,
        );
    }

    public function toAst() : RefreshMatViewStmt
    {
        if ($this->name === null || $this->name === '') {
            throw InvalidExpressionException::invalidValue('materialized view name', 'null or empty');
        }

        $stmt = new RefreshMatViewStmt();

        $rangeVar = new RangeVar();
        $rangeVar->setRelname($this->name);
        $rangeVar->setRelpersistence('p');
        $rangeVar->setInh(true);

        if ($this->schema !== null) {
            $rangeVar->setSchemaname($this->schema);
        }

        $stmt->setRelation($rangeVar);

        if ($this->concurrent) {
            $stmt->setConcurrent(true);
        }

        if ($this->skipData !== null) {
            $stmt->setSkipData($this->skipData);
        }

        return $stmt;
    }

    public function withData() : RefreshMatViewFinalStep
    {
        return new self(
            $this->name,
            $this->schema,
            $this->concurrent,
            false,
        );
    }

    public function withNoData() : RefreshMatViewFinalStep
    {
        return new self(
            $this->name,
            $this->schema,
            $this->concurrent,
            true,
        );
    }
}
