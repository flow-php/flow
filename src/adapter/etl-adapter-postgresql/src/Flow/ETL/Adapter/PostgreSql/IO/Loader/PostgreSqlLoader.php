<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\IO\Loader;

use Flow\ETL\Adapter\PostgreSql\IO\Loader\LoaderOptions\DeleteOptions;
use Flow\ETL\Adapter\PostgreSql\IO\Loader\LoaderOptions\InsertOptions;
use Flow\ETL\Adapter\PostgreSql\IO\Loader\LoaderOptions\UpdateOptions;
use Flow\ETL\Adapter\PostgreSql\IO\Loader\QueryBuilder\DeleteQueryBuilder;
use Flow\ETL\Adapter\PostgreSql\IO\Loader\QueryBuilder\InsertQueryBuilder;
use Flow\ETL\Adapter\PostgreSql\IO\Loader\QueryBuilder\UpdateQueryBuilder;
use Flow\ETL\Adapter\PostgreSql\Values\Schema\TypesMap;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\IO\Loader;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\PostgreSql\Client\Client;
use SensitiveParameter;
use Throwable;

use function intdiv;
use function max;

/**
 * PostgreSQL loader for ETL pipelines.
 *
 * Supports INSERT, UPDATE, and DELETE operations. UPDATE and DELETE issue one statement per row, so only
 * INSERT can reach the bind-parameter cap and is chunked.
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
        private TypesMap $typesMap = new TypesMap(),
    ) {
        $this->client->converters()->register(new EnumConverter());
        $this->client->converters()->register(new XMLConverter());
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        throw InvalidArgumentException::because('%s holds live state and is never described', self::class);
    }

    public function describe(): ?Description
    {
        return null;
    }

    public function load(Rows $rows, Backend $backend): void
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

    public function withTypesMap(TypesMap $typesMap): self
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

    private function deleteRows(Rows $rows): void
    {
        $deleteOptions = $this->deleteOptions;

        if ($deleteOptions === null) {
            throw new RuntimeException('DeleteOptions must be set for DELETE operation');
        }

        $builder = new DeleteQueryBuilder($this->table, $this->typesMap);
        $schema = $rows->schema();

        foreach ($this->encoder()->encode($rows) as $values) {
            [$query, $params] = $builder->build($values, $schema, $deleteOptions);
            $this->client->execute($query, $params);
        }
    }

    private function insertRows(Rows $rows): void
    {
        // PQ_QUERY_PARAM_MAX_LIMIT: an INSERT binds one parameter per column per row
        $maxRows = max(1, intdiv(65_535, $rows->schema()->count()));

        if ($rows->count() <= $maxRows) {
            $builder = new InsertQueryBuilder($this->table, $this->typesMap);
            [$query, $params] = $builder->build(
                $this->encoder()->columns($rows),
                $rows->count(),
                $rows->schema(),
                $this->client->converters(),
                $this->insertOptions,
            );
            $this->client->execute($query, $params);

            return;
        }

        // the same nesting check the cursor extractor uses, so a transactional wrapper keeps owning its transaction
        $ownTransaction = $this->client->getTransactionNestingLevel() === 0;

        if ($ownTransaction) {
            $this->client->beginTransaction();
        }

        try {
            foreach ($rows->chunks($maxRows) as $chunk) {
                $builder = new InsertQueryBuilder($this->table, $this->typesMap);
                [$query, $params] = $builder->build(
                    $this->encoder()->columns($chunk),
                    $chunk->count(),
                    $chunk->schema(),
                    $this->client->converters(),
                    $this->insertOptions,
                );
                $this->client->execute($query, $params);
            }

            if ($ownTransaction) {
                $this->client->commit();
            }
        } catch (Throwable $failure) {
            if ($ownTransaction) {
                $this->client->rollBack();
            }

            throw $failure;
        }
    }

    private function updateRows(Rows $rows): void
    {
        $updateOptions = $this->updateOptions;

        if ($updateOptions === null) {
            throw new RuntimeException('UpdateOptions must be set for UPDATE operation');
        }

        $builder = new UpdateQueryBuilder($this->table, $this->typesMap);
        $schema = $rows->schema();

        foreach ($this->encoder()->encode($rows) as $values) {
            [$query, $params] = $builder->build($values, $schema, $updateOptions);

            if ($query !== null) {
                $this->client->execute($query, $params);
            }
        }
    }
}
