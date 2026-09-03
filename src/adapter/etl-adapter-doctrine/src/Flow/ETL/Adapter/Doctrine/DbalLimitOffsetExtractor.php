<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Query\QueryBuilder;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Generator;

use function count;
use function is_numeric;

final class DbalLimitOffsetExtractor implements Extractor
{
    private ?int $maximum = null;

    private int $offset = 0;

    private int $pageSize = 1000;

    private ?Schema $schema = null;

    private ?Schema $derived = null;

    public function __construct(
        private readonly Connection $connection,
        private readonly QueryBuilder $queryBuilder,
    ) {}

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

    /**
     * @return Generator<int, Rows, Signal|null, void>
     */
    public function extract(FlowContext $context): Generator
    {
        $schema = $this->schema();

        if ($this->maximum === null && $this->queryBuilder->getMaxResults()) {
            $this->maximum = $this->queryBuilder->getMaxResults();
        }

        if ($this->offset === 0 && $this->queryBuilder->getFirstResult()) {
            $this->offset = $this->queryBuilder->getFirstResult();
        }

        if (isset($this->maximum)) {
            $total = $this->maximum;
        } else {
            $countQuery = (clone $this->queryBuilder)->select('COUNT(*)');

            $nonGroupByQuery = (clone $this->queryBuilder)->select('COUNT(*)')->resetGroupBy();

            if ($countQuery->getSQL() === $nonGroupByQuery->getSQL()) {
                $countQuery->resetOrderBy();

                $totalValue = $this->connection->fetchOne(
                    $countQuery->getSQL(),
                    $countQuery->getParameters(),
                    $countQuery->getParameterTypes(),
                );
                $total = is_numeric($totalValue) ? (int) $totalValue : 0;
            } else {
                // @mago-expect analysis:mixed-assignment
                $totalValue = $this->connection
                    ->executeQuery(
                        'SELECT COUNT(*) FROM (' . $countQuery->getSQL() . ') as count_query',
                        $countQuery->getParameters(),
                        $countQuery->getParameterTypes(),
                    )
                    ->fetchOne();
                $total = is_numeric($totalValue) ? (int) $totalValue : 0;
            }
        }

        $totalFetched = 0;
        $encoder = new DbalEncoder();

        for ($page = 0; $page < (new Pages($total, $this->pageSize))->pages(); $page++) {
            $offset = ($page * $this->pageSize) + $this->offset;

            $pageQuery = (clone $this->queryBuilder)->setMaxResults($this->pageSize)->setFirstResult($offset);

            $pageResults = $this->connection
                ->executeQuery($pageQuery->getSQL(), $pageQuery->getParameters(), $pageQuery->getParameterTypes())
                ->fetchAllAssociative();

            $rawBatch = [];

            foreach ($pageResults as $row) {
                $rawBatch[] = $row;
            }

            $hydrated = $context->hydrator()->cast($encoder->decode($rawBatch), $schema);

            foreach ($hydrated as $hydratedRow) {
                $signal = yield Rows::trusted($hydrated->schema(), [$hydratedRow]);

                $totalFetched++;

                if ($signal === Signal::STOP) {
                    return;
                }

                if (null !== $this->maximum && $totalFetched >= $this->maximum) {
                    return;
                }
            }
        }
    }

    public function schema(): Schema
    {
        return (
            $this->schema ?? ($this->derived ??= (new DbalResultSchema())->of(
                $this->connection,
                $this->queryBuilder->getSQL(),
                self::class,
            ))
        );
    }

    public function withMaximum(int $maximum): self
    {
        if ($maximum <= 0) {
            throw new InvalidArgumentException('Maximum must be greater than 0, got ' . $maximum);
        }

        $this->maximum = $maximum;

        return $this;
    }

    public function withOffset(int $offset): self
    {
        if ($offset < 0) {
            throw new InvalidArgumentException('Offset must be greater than 0, got ' . $offset);
        }

        $this->offset = $offset;

        return $this;
    }

    public function withPageSize(int $pageSize): self
    {
        if ($pageSize <= 0) {
            throw new InvalidArgumentException('Page size must be greater than 0, got ' . $pageSize);
        }

        $this->pageSize = $pageSize;

        return $this;
    }

    public function withSchema(Schema $schema): static
    {
        $this->schema = $schema;

        return $this;
    }
}
