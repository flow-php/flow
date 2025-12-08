<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\View\RefreshMaterializedView;

use Flow\PgQuery\Protobuf\AST\{RangeVar, RefreshMatViewStmt};
use Flow\PgQuery\QueryBuilder\Exception\InvalidExpressionException;

final readonly class RefreshMaterializedViewBuilder implements RefreshMatViewFinalStep, RefreshMatViewOptionsStep
{
    private function __construct(
        private ?string $name = null,
        private ?string $schema = null,
        private bool $concurrent = false,
        private ?bool $skipData = null,
    ) {
    }

    public static function create(string $name, ?string $schema = null) : RefreshMatViewOptionsStep
    {
        $parts = \explode('.', $name);

        if (\count($parts) === 2) {
            return new self($parts[1], $parts[0]);
        }

        return new self($name, $schema);
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
