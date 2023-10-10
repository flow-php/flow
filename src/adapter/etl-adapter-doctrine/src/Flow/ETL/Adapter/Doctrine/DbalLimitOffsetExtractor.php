<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use function Flow\ETL\DSL\array_to_rows;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Query\QueryBuilder;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;

final class DbalLimitOffsetExtractor implements Extractor
{
    public function __construct(
        private readonly Connection $connection,
        private readonly QueryBuilder $queryBuilder,
        private readonly int $pageSize = 1000,
        private readonly ?int $maximum = null,
        private readonly Row\EntryFactory $entryFactory = new Row\Factory\NativeEntryFactory()
    ) {
    }

    /**
     * @param array<OrderBy> $orderBy
     */
    public static function table(
        Connection $connection,
        Table $table,
        array $orderBy,
        int $pageSize = 1000,
        ?int $maximum = null,
        Row\EntryFactory $entryFactory = new Row\Factory\NativeEntryFactory()
    ) : self {
        if (!\count($orderBy)) {
            throw new InvalidArgumentException('There must be at least one column to order by, zero given');
        }

        $queryBuilder = $connection->createQueryBuilder()
            ->select($table->columns ?: '*')
            ->from($table->name);

        foreach ($orderBy as $order) {
            $queryBuilder = $queryBuilder->orderBy($order->column, $order->order->name);
        }

        return new self(
            $connection,
            $queryBuilder,
            $pageSize,
            $maximum,
            $entryFactory
        );
    }

    public function extract(FlowContext $context) : \Generator
    {
        if (isset($this->maximum)) {
            $total = $this->maximum;
        } else {
            $countQuery = (clone $this->queryBuilder)->select('COUNT(*)');
            $countQuery->resetQueryPart('orderBy');

            /** @phpstan-ignore-next-line */
            $total = (int) $this->connection->fetchOne(
                $countQuery->getSQL(),
                $countQuery->getParameters(),
                $countQuery->getParameterTypes()
            );
        }

        $totalFetched = 0;

        for ($page = 0; $page <= (new Pages($total, $this->pageSize))->pages(); $page++) {
            $offset = $page * $this->pageSize;

            $pageQuery = $this->queryBuilder
                ->setMaxResults($this->pageSize)
                ->setFirstResult($offset);

            $rows = [];

            $pageResults = $this->connection->executeQuery(
                $pageQuery->getSQL(),
                $pageQuery->getParameters(),
                $pageQuery->getParameterTypes()
            )->fetchAllAssociative();

            foreach ($pageResults as $row) {
                $rows[] = $row;

                $totalFetched++;

                if (null !== $this->maximum && $totalFetched >= $this->maximum) {
                    break;
                }
            }

            yield array_to_rows($rows, $this->entryFactory);
        }
    }
}
