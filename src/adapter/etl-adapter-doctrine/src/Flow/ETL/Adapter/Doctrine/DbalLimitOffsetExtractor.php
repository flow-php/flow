<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Query\QueryBuilder;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\BatchableExtractor;
use Flow\ETL\Extractor\Batches;
use Flow\ETL\Extractor\LimitPushDown;
use Flow\ETL\Extractor\PushesLimit;
use Flow\ETL\Extractor\RewindableExtractor;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Generator;

use function count;
use function is_numeric;
use function min;

final class DbalLimitOffsetExtractor implements BatchableExtractor, Extractor, LimitPushDown, RewindableExtractor
{
    use Batches;
    use PushesLimit;

    private ?int $maximum = null;

    private int $offset = 0;

    private ?Schema $schema = null;

    private ?Schema $derived = null;

    public function __construct(
        private readonly Connection $connection,
        private readonly QueryBuilder $queryBuilder,
    ) {
        // a page is a network round trip, not a buffer: 100 would cost 10x the round trips
        $this->batchSize = 1_000;
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
    public function extract(FlowContext $context): Generator
    {
        $schema = $this->schema();

        if ($this->maximum === null && $this->queryBuilder->getMaxResults()) {
            $this->maximum = $this->queryBuilder->getMaxResults();
        }

        if ($this->offset === 0 && $this->queryBuilder->getFirstResult()) {
            $this->offset = $this->queryBuilder->getFirstResult();
        }

        $pushed = $this->pushedLimit();
        $maximum = match (true) {
            $this->maximum !== null && $pushed !== null => min($this->maximum, $pushed),
            $this->maximum !== null => $this->maximum,
            default => $pushed,
        };

        if (null !== $maximum) {
            $total = $maximum;
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

        $yielded = 0;
        $encoder = new DbalEncoder();

        for ($page = 0; $page < (new Pages($total, $this->batchSize))->pages(); $page++) {
            // the request asks only for what is still wanted, while the offset keeps striding by the batch size
            $pageSize = $maximum === null ? $this->batchSize : min($this->batchSize, $maximum - $yielded);
            $offset = ($page * $this->batchSize) + $this->offset;

            $pageQuery = (clone $this->queryBuilder)->setMaxResults($pageSize)->setFirstResult($offset);

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

            $hydrated = $context->hydrator()->hydrate($encoder->decode($rawBatch), $schema);

            $yielded += $hydrated->count();

            $signal = yield $hydrated;

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

    public function withSchema(Schema $schema): static
    {
        $this->schema = $schema;

        return $this;
    }
}
