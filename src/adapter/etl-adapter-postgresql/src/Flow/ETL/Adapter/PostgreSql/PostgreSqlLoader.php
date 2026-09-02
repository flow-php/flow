<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql;

use Flow\ETL\Adapter\PostgreSql\Exception\RuntimeException;
use Flow\ETL\Adapter\PostgreSql\LoaderOptions\DeleteOptions;
use Flow\ETL\Adapter\PostgreSql\LoaderOptions\InsertOptions;
use Flow\ETL\Adapter\PostgreSql\LoaderOptions\UpdateOptions;
use Flow\ETL\Adapter\PostgreSql\QueryBuilder\DeleteQueryBuilder;
use Flow\ETL\Adapter\PostgreSql\QueryBuilder\InsertQueryBuilder;
use Flow\ETL\Adapter\PostgreSql\QueryBuilder\UpdateQueryBuilder;
use Flow\ETL\Adapter\PostgreSql\ValueConverter\EnumConverter;
use Flow\ETL\Adapter\PostgreSql\ValueConverter\XMLConverter;
use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Rows;
use Flow\PostgreSql\Client\Client;
use Throwable;

/**
 * PostgreSQL loader for ETL pipelines.
 *
 * Supports INSERT, UPDATE, and DELETE operations.
 */
final class PostgreSqlLoader implements Loader
{
    private ?DeleteOptions $deleteOptions = null;

    private ?PostgreSqlEncoder $encoder = null;

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

    public function load(Rows $rows, FlowContext $context): void
    {
        if ($rows->count() === 0) {
            return;
        }

        $context->telemetry()->loadingStarted($this);

        try {
            match ($this->operation) {
                Operation::INSERT => $this->insertRows($rows, $context),
                Operation::UPDATE => $this->updateRows($rows, $context),
                Operation::DELETE => $this->deleteRows($rows, $context),
            };

            $context->telemetry()->loadingCompleted($this, [TelemetryAttributes::ATTR_LOADING_ROWS => $rows->count()]);
        } catch (Throwable $e) {
            $context->telemetry()->loadingFailed($this, $e);

            throw $e;
        }
    }

    public function withDeleteOptions(DeleteOptions $options): self
    {
        $this->deleteOptions = $options;

        return $this;
    }

    public function withInsertOptions(InsertOptions $options): self
    {
        $this->insertOptions = $options;

        return $this;
    }

    public function withOperation(Operation $operation): self
    {
        $this->operation = $operation;

        return $this;
    }

    public function withTypesMap(EntryTypesMap $typesMap): self
    {
        $this->typesMap = $typesMap;

        return $this;
    }

    public function withUpdateOptions(UpdateOptions $options): self
    {
        $this->updateOptions = $options;

        return $this;
    }

    private function encoder(): PostgreSqlEncoder
    {
        return $this->encoder ??= new PostgreSqlEncoder();
    }

    private function deleteRows(Rows $rows, FlowContext $context): void
    {
        $deleteOptions = $this->deleteOptions;

        if ($deleteOptions === null) {
            throw new RuntimeException('DeleteOptions must be set for DELETE operation');
        }

        $builder = new DeleteQueryBuilder($this->table, $this->typesMap);
        $schema = $rows->schema();

        foreach ($this->encoder()->encode($context->hydrator()->dehydrate($rows)) as $values) {
            [$query, $params] = $builder->build($values, $schema, $deleteOptions);
            $this->client->execute($query, $params);
        }
    }

    private function insertRows(Rows $rows, FlowContext $context): void
    {
        $builder = new InsertQueryBuilder($this->table, $this->typesMap);
        $values = $this->encoder()->encode($context->hydrator()->dehydrate($rows));

        [$query, $params] = $builder->build($values, $rows->schema(), $this->insertOptions);
        $this->client->execute($query, $params);
    }

    private function updateRows(Rows $rows, FlowContext $context): void
    {
        $updateOptions = $this->updateOptions;

        if ($updateOptions === null) {
            throw new RuntimeException('UpdateOptions must be set for UPDATE operation');
        }

        $builder = new UpdateQueryBuilder($this->table, $this->typesMap);
        $schema = $rows->schema();

        foreach ($this->encoder()->encode($context->hydrator()->dehydrate($rows)) as $values) {
            [$query, $params] = $builder->build($values, $schema, $updateOptions);

            if ($query !== null) {
                $this->client->execute($query, $params);
            }
        }
    }
}
