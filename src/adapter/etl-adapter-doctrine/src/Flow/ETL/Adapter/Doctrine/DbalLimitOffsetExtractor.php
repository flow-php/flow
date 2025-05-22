<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use function Flow\ETL\DSL\array_to_rows;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Query\QueryBuilder;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\SQL\QueryBuilder\Adapter\DbalQueryBuilderAdapter;
use Flow\ETL\SQL\QueryBuilder\Adapter\FlowQueryBuilderWrapper;
use Flow\ETL\SQL\QueryBuilder\QueryBuilderInterface;
use Flow\ETL\{Extractor, FlowContext, Schema};

final class DbalLimitOffsetExtractor implements Extractor
{
    private ?int $maximum = null;

    private int $offset = 0;

    private int $pageSize = 1000;

    private ?Schema $schema = null;

    private readonly QueryBuilderInterface $flowQueryBuilder;

    public function __construct(
        private readonly Connection $connection,
        private readonly QueryBuilder|QueryBuilderInterface $queryBuilder,
    ) {
        if ($queryBuilder instanceof QueryBuilderInterface) {
            $this->flowQueryBuilder = $queryBuilder;
        } elseif ($queryBuilder instanceof FlowQueryBuilderWrapper) {
            $this->flowQueryBuilder = $queryBuilder->getFlowQueryBuilder();
        } else {
            $this->flowQueryBuilder = DbalQueryBuilderAdapter::fromDbalQueryBuilder($queryBuilder);
        }
    }

    /**
     * @param array<OrderBy> $orderBy
     */
    public static function table(
        Connection $connection,
        Table $table,
        array $orderBy,
    ) : self {
        if (!\count($orderBy)) {
            throw new InvalidArgumentException('There must be at least one column to order by, zero given');
        }

        $queryBuilder = $connection->createQueryBuilder()
            ->select(...$table->columns ?: ['*'])
            ->from($table->name);

        foreach ($orderBy as $order) {
            $queryBuilder = $queryBuilder->orderBy($order->column, $order->order->name);
        }

        return new self(
            $connection,
            $queryBuilder,
        );
    }

    public function extract(FlowContext $context) : \Generator
    {
        // Get initial limit and offset from query builder if not set
        if ($this->maximum === null) {
            $queryParts = $this->flowQueryBuilder->getQueryParts();
            if ($queryParts['limit'] !== null) {
                $this->maximum = $queryParts['limit'];
            }
        }

        if ($this->offset === 0) {
            $queryParts = $this->flowQueryBuilder->getQueryParts();
            if ($queryParts['offset'] !== null) {
                $this->offset = $queryParts['offset'];
            }
        }

        if (isset($this->maximum)) {
            $total = $this->maximum;
        } else {
            // Create count query
            $countQuery = $this->flowQueryBuilder->clone()
                ->select('COUNT(*)')
                ->resetQueryPart('orderBy');

            // Check if we have GROUP BY
            $queryParts = $countQuery->getQueryParts();
            $hasGroupBy = !empty($queryParts['groupBy']);

            if (!$hasGroupBy) {
                // Simple count query
                $total = (int) $this->connection->fetchOne(
                    $countQuery->toSQL(),
                    $countQuery->getParameters(),
                    $countQuery->getParameterTypes()
                );
            } else {
                // For grouped queries, wrap in a subquery to get accurate count
                $countQuery->resetQueryPart('groupBy');
                $total = (int) $this->connection->executeQuery(
                    'SELECT COUNT(*) FROM (' . $this->flowQueryBuilder->toSQL() . ') as count_query',
                    $this->flowQueryBuilder->getParameters(),
                    $this->flowQueryBuilder->getParameterTypes()
                )->fetchOne();
            }
        }

        $totalFetched = 0;

        for ($page = 0; $page < (new Pages($total, $this->pageSize))->pages(); $page++) {
            $offset = $page * $this->pageSize + $this->offset;

            // Clone the query builder for this page
            $pageQuery = $this->flowQueryBuilder->clone()
                ->limit($this->pageSize)
                ->offset($offset);

            $pageResults = $this->connection->executeQuery(
                $pageQuery->toSQL(),
                $pageQuery->getParameters(),
                $pageQuery->getParameterTypes()
            )->fetchAllAssociative();

            foreach ($pageResults as $row) {
                $signal = yield array_to_rows($row, $context->entryFactory(), [], $this->schema);

                if ($signal === Signal::STOP) {
                    return;
                }

                $totalFetched++;

                if (null !== $this->maximum && $totalFetched >= $this->maximum) {
                    break;
                }
            }
        }
    }

    public function withMaximum(int $maximum) : self
    {
        if ($maximum <= 0) {
            throw new InvalidArgumentException('Maximum must be greater than 0, got ' . $maximum);
        }

        $this->maximum = $maximum;

        return $this;
    }

    public function withOffset(int $offset) : self
    {
        if ($offset < 0) {
            throw new InvalidArgumentException('Offset must be greater than 0, got ' . $offset);
        }

        $this->offset = $offset;

        return $this;
    }

    public function withPageSize(int $pageSize) : self
    {
        if ($pageSize <= 0) {
            throw new InvalidArgumentException('Page size must be greater than 0, got ' . $pageSize);
        }

        $this->pageSize = $pageSize;

        return $this;
    }

    public function withSchema(Schema $schema) : self
    {
        $this->schema = $schema;

        return $this;
    }
}
