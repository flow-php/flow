<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql;

use Flow\ETL\Adapter\PostgreSql\Exception\RuntimeException;
use Flow\ETL\Adapter\PostgreSql\LoaderOptions\{DeleteOptions, InsertOptions, UpdateOptions};
use Flow\ETL\Adapter\PostgreSql\QueryBuilder\{DeleteQueryBuilder, InsertQueryBuilder, UpdateQueryBuilder};
use Flow\ETL\Adapter\PostgreSql\ValueConverter\{EnumConverter, XMLConverter};
use Flow\ETL\{FlowContext, Loader, Rows};
use Flow\PostgreSql\Client\Client;

/**
 * PostgreSQL loader for ETL pipelines.
 *
 * Supports INSERT, UPDATE, and DELETE operations.
 */
final class PostgreSqlLoader implements Loader
{
    private ?DeleteOptions $deleteOptions = null;

    private ?InsertOptions $insertOptions = null;

    private Operation $operation = Operation::INSERT;

    private ?UpdateOptions $updateOptions = null;

    public function __construct(
        private readonly Client $client,
        private readonly string $table,
        private EntryTypesMap $typesMap = new EntryTypesMap(),
    ) {
        $this->client->converters()->register(new EnumConverter());
        $this->client->converters()->register(new XMLConverter());
    }

    public function load(Rows $rows, FlowContext $context) : void
    {
        if ($rows->count() === 0) {
            return;
        }

        match ($this->operation) {
            Operation::INSERT => $this->insertRows($rows),
            Operation::UPDATE => $this->updateRows($rows),
            Operation::DELETE => $this->deleteRows($rows),
        };
    }

    public function withDeleteOptions(DeleteOptions $options) : self
    {
        $this->deleteOptions = $options;

        return $this;
    }

    public function withInsertOptions(InsertOptions $options) : self
    {
        $this->insertOptions = $options;

        return $this;
    }

    public function withOperation(Operation $operation) : self
    {
        $this->operation = $operation;

        return $this;
    }

    public function withTypesMap(EntryTypesMap $typesMap) : self
    {
        $this->typesMap = $typesMap;

        return $this;
    }

    public function withUpdateOptions(UpdateOptions $options) : self
    {
        $this->updateOptions = $options;

        return $this;
    }

    private function deleteRows(Rows $rows) : void
    {
        if ($this->deleteOptions === null) {
            throw new RuntimeException('DeleteOptions must be set for DELETE operation');
        }

        $builder = new DeleteQueryBuilder($this->table, $this->typesMap);

        foreach ($rows as $row) {
            [$query, $params] = $builder->build($row, $this->deleteOptions);
            $this->client->execute($query, $params);
        }
    }

    private function insertRows(Rows $rows) : void
    {
        $builder = new InsertQueryBuilder($this->table, $this->typesMap);
        [$query, $params] = $builder->build($rows, $this->insertOptions);
        $this->client->execute($query, $params);
    }

    private function updateRows(Rows $rows) : void
    {
        if ($this->updateOptions === null) {
            throw new RuntimeException('UpdateOptions must be set for UPDATE operation');
        }

        $builder = new UpdateQueryBuilder($this->table, $this->typesMap);

        foreach ($rows as $row) {
            [$query, $params] = $builder->build($row, $this->updateOptions);

            if ($query !== null) {
                $this->client->execute($query, $params);
            }
        }
    }
}
