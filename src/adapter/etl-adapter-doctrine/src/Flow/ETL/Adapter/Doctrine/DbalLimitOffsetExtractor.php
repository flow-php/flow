<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Query\QueryBuilder;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;

final class DbalLimitOffsetExtractor implements Extractor
{
    public function __construct(
        private readonly Connection $connection,
        private readonly QueryBuilder $queryBuilder,
        private readonly int $pageSize = 1000,
        private readonly ?int $maximum = null,
        private readonly string $rowEntryName = 'row',
    ) {
        if ($this->queryBuilder->getQueryPart('orderBy') === []) { //we could check for some other required query parts same way, like FROM or SELECT
            throw new InvalidArgumentException('There must be at least one column to order by, zero given');
        }

        if ($this->queryBuilder->getQueryPart('select') === []) { //we could check for some other required query parts same way, like FROM or SELECT
            throw new InvalidArgumentException('There must be at least one column to select from, zero given');
        }

        if ($this->queryBuilder->getQueryPart('from') === []) { //we could check for some other required query parts same way, like FROM or SELECT
            throw new InvalidArgumentException('There must be table to select from, none given');
        }
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