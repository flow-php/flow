<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use Doctrine\DBAL\Connection;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;

/**
 * @psalm-immutable
 */
final class DbalLimitOffsetExtractor implements Extractor
{
    /**
     * @param array<OrderBy> $orderBy
     */
    public function __construct(
        private readonly Connection $connection,
        private readonly Table $table,
        private readonly array $orderBy,
        private readonly int $pageSize = 1000,
        private readonly ?int $maximum = null,
        private readonly string $rowEntryName = 'row'
    ) {
        if (!\count($this->orderBy)) {
            throw new InvalidArgumentException('There must be at least one column to order by, zero given');
        }
    }

    /**
     * @psalm-suppress ImpureMethodCall
     */
    public function extract(FlowContext $context) : \Generator
    {
        if (isset($this->maximum)) {
            $total = $this->maximum;
        } else {
            $countQuery = $this->connection->createQueryBuilder()->select('COUNT(*)')
                ->from($this->table->name);

            /** @phpstan-ignore-next-line */
            $total = (int) $this->connection->fetchOne($countQuery->getSQL(), $countQuery->getParameters(), $countQuery->getParameterTypes());
        }

        $totalFetched = 0;

        for ($page = 0; $page <= (new Pages($total, $this->pageSize))->pages(); $page++) {
            $offset = $page * $this->pageSize;

            $pageQuery = $this->connection->createQueryBuilder()->select('*')
                ->from($this->table->name)
                ->setMaxResults($this->pageSize)
                ->setFirstResult($offset);

            if (\is_array($this->table->columns) && \count($this->table->columns)) {
                $pageQuery = $pageQuery->select($this->table->columns);
            }

            foreach ($this->orderBy as $orderBy) {
                $pageQuery = $pageQuery->orderBy($orderBy->column, $orderBy->order->name);
            }

            $rows = [];

            $pageResults = $this->connection->executeQuery(
                $pageQuery->getSQL(),
                $pageQuery->getParameters(),
                $pageQuery->getParameterTypes()
            )->fetchAllAssociative();

            foreach ($pageResults as $row) {
                $rows[] = Row::create(new Row\Entry\ArrayEntry($this->rowEntryName, $row));

                $totalFetched++;

                if (null !== $this->maximum && $totalFetched >= $this->maximum) {
                    break;
                }
            }

            yield new Rows(...$rows);
        }
    }
}
