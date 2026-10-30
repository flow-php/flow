<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\IO\Extractor;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Query\QueryBuilder;
use Flow\ETL\Adapter\Doctrine\IO\Extractor\Explain\ExplainedRows;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Exception\SchemaNotDerivableException;
use Flow\ETL\IO\Extractor;
use Flow\ETL\IO\Extractor\BatchableExtractor;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\IO\Extractor\Statistics;
use Flow\ETL\Values\BatchSize;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Rows\RowsBuilder;
use Flow\ETL\Values\Schema;
use Generator;

use function count;
use function is_numeric;
use function max;
use function min;

final class DbalLimitOffsetExtractor implements BatchableExtractor, Extractor
{
    private BatchSize $batchSize;

    private ?int $maximum = null;

    private ?Schema $schema = null;

    private ?Schema $derived = null;

    private ?SchemaNotDerivableException $refusal = null;

    private ?Statistics $statistics = null;

    public function __construct(
        private readonly Connection $connection,
        private readonly QueryBuilder $queryBuilder,
    ) {
        // a page is a network round trip, not a buffer: 100 would cost 10x the round trips
        $this->batchSize = new BatchSize(1_000);
    }

    /**
     * @return int<1, max>
     */
    public function batchSize(): int
    {
        return $this->batchSize->size;
    }

    /**
     * @param array<OrderBy> $orderBy
     */
    public static function table(Connection $connection, Table $table, array $orderBy): self
    {
        if (!count($orderBy)) {
            throw new InvalidArgumentException('There must be at least one column to order by, zero given');
        }

        $queryBuilder = $connection
            ->createQueryBuilder()
            ->select(...$table->columns ?: ['*'])
            ->from($table->name);

        foreach ($orderBy as $order) {
            $queryBuilder = $queryBuilder->addOrderBy($order->column, $order->order->name);
        }

        return new self($connection, $queryBuilder);
    }

    public function isRepeatable(): bool
    {
        return true;
    }

    /**
     * @return Generator<int, Rows, Signal|null, void>
     */
    public function extract(Backend $backend, Criteria $criteria): Generator
    {
        $schema = $this->schema();

        if ($this->maximum === null && $this->queryBuilder->getMaxResults()) {
            $this->maximum = $this->queryBuilder->getMaxResults();
        }

        $offset = $this->queryBuilder->getFirstResult();
        $maximum = match (true) {
            $this->maximum !== null && $criteria->limit !== null => min($this->maximum, $criteria->limit),
            $this->maximum !== null => $this->maximum,
            default => $criteria->limit,
        };

        if (null !== $maximum) {
            $total = $maximum;
        } else {
            // the count covers the rows before the offset too, so it runs without it
            $countQuery = (clone $this->queryBuilder)->select('COUNT(*)')->setFirstResult(0);

            $nonGroupByQuery = (clone $this->queryBuilder)->select('COUNT(*)')->setFirstResult(0)->resetGroupBy();

            if ($countQuery->getSQL() === $nonGroupByQuery->getSQL()) {
                $countQuery->resetOrderBy();

                $totalValue = $this->connection->fetchOne(
                    $countQuery->getSQL(),
                    $countQuery->getParameters(),
                    $countQuery->getParameterTypes(),
                );
                $total = max(0, (is_numeric($totalValue) ? (int) $totalValue : 0) - $offset);
            } else {
                // @mago-expect analysis:mixed-assignment
                $totalValue = $this->connection
                    ->executeQuery(
                        'SELECT COUNT(*) FROM (' . $countQuery->getSQL() . ') as count_query',
                        $countQuery->getParameters(),
                        $countQuery->getParameterTypes(),
                    )
                    ->fetchOne();
                $total = max(0, (is_numeric($totalValue) ? (int) $totalValue : 0) - $offset);
            }
        }

        $yielded = 0;

        for ($page = 0; $page < (new Pages($total, $this->batchSize->size))->pages(); $page++) {
            // the request asks only for what is still wanted, while the offset keeps striding by the batch size
            $pageSize = $maximum === null ? $this->batchSize->size : min($this->batchSize->size, $maximum - $yielded);

            $pageQuery = (clone $this->queryBuilder)
                ->setMaxResults($pageSize)
                ->setFirstResult(($page * $this->batchSize->size) + $offset);

            $pageResults = $this->connection
                ->executeQuery($pageQuery->getSQL(), $pageQuery->getParameters(), $pageQuery->getParameterTypes())
                ->fetchAllAssociative();

            if ($pageResults === []) {
                return;
            }

            $rawBatch = [];

            foreach ($pageResults as $row) {
                $rawBatch[] = $row;
            }

            $rows = (new RowsBuilder($schema, $backend))
                ->appendRows($rawBatch)
                ->finish();

            $yielded += $rows->count();

            $signal = yield $rows;

            if ($signal === Signal::STOP) {
                return;
            }

            // a short page means the source ran out, whatever $total promised
            if (count($pageResults) < $pageSize) {
                return;
            }
        }
    }

    public function schema(): Schema
    {
        if ($this->schema !== null) {
            return $this->schema;
        }

        if ($this->refusal !== null) {
            throw $this->refusal;
        }

        try {
            return $this->derived ??= (new DbalResultSchema())->of(
                $this->connection,
                $this->queryBuilder->getSQL(),
                self::class,
            );
        } catch (SchemaNotDerivableException $refusal) {
            $this->refusal = $refusal;

            throw $refusal;
        }
    }

    /**
     * The plan already applies the query builder's own limit and offset.
     */
    public function statistics(): Statistics
    {
        return $this->statistics ??= new Statistics(rows: (new ExplainedRows())->of(
            $this->connection,
            $this->queryBuilder->getSQL(),
            $this->queryBuilder->getParameters(),
            $this->queryBuilder->getParameterTypes(),
            $this->maximum ?? $this->queryBuilder->getMaxResults(),
        ));
    }

    public function withBatchSize(int $batchSize): static
    {
        $this->batchSize = new BatchSize($batchSize);

        return $this;
    }

    public function withMaximum(int $maximum): self
    {
        if ($maximum <= 0) {
            throw new InvalidArgumentException('Maximum must be greater than 0, got ' . $maximum);
        }

        $this->maximum = $maximum;
        $this->statistics = null;

        return $this;
    }

    public function withSchema(Schema $schema): static
    {
        $this->schema = $schema;

        return $this;
    }
}
