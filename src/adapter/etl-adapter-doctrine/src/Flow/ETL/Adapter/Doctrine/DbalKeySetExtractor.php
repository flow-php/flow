<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use function Flow\ETL\DSL\array_to_rows;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Query\QueryBuilder;
use Flow\ETL\{Adapter\Doctrine\Pagination\KeySet, Extractor, FlowContext, Schema};
use Flow\ETL\Exception\{InvalidArgumentException, RuntimeException};

/**
 * Extractor implementing keyset pagination for Doctrine DBAL queries.
 *
 * This extractor fetches rows page by page using keyset pagination, which is more efficient
 * than limit/offset for large datasets. It requires a KeySet object defining the columns
 * and sort orders for pagination. The key columns must be non-null and provide a unique
 * ordering to ensure correct pagination.
 */
final class DbalKeySetExtractor implements Extractor
{
    private ?int $maximum = null;

    private int $pageSize = 1000;

    private ?Schema $schema = null;

    public function __construct(
        private readonly Connection $connection,
        private readonly QueryBuilder $queryBuilder,
        private readonly KeySet $keySet,
    ) {
        $qb = clone $this->queryBuilder;

        /** @phpstan-ignore-next-line */
        $cleanQuery = \method_exists($qb, 'resetOrderBy') ? (clone $this->queryBuilder)->resetOrderBy() : (clone $qb)->resetQueryPart('orderBy');

        if ($cleanQuery->getSQL() !== $this->queryBuilder->getSQL()) {
            throw new InvalidArgumentException('Keyset pagination cannot be used with an ORDER BY clause, please remove OrderBy from Query Builder');
        }

        if (empty($this->keySet->keys)) {
            throw new InvalidArgumentException('KeySet must contain at least one key for pagination');
        }
    }

    public function extract(FlowContext $context) : \Generator
    {
        $totalFetched = 0;
        $lastRow = null;

        while (true) {
            $qb = clone $this->queryBuilder;
            $qb->setMaxResults($this->pageSize);

            foreach ($this->keySet->keys as $key) {
                $qb->addOrderBy($key->column, $key->order->value);
            }

            if ($lastRow !== null) {
                $conditions = [];
                $parameters = [];
                $parameterTypes = [];

                foreach ($this->keySet->keys as $index => $key) {
                    if (!\array_key_exists($key->column, $lastRow)) {
                        throw new RuntimeException(sprintf('Column "%s" not found in last row for keyset pagination', $key->column));
                    }

                    $lastValue = $lastRow[$key->column];

                    if ($lastValue === null) {
                        throw new RuntimeException(sprintf('NULL value found in column "%s" for keyset pagination; key columns must be non-null', $key->column));
                    }

                    $paramName = $key->column . '_previous';
                    $parameters[$paramName] = $lastValue;
                    $parameterTypes[$paramName] = $key->type;

                    $subConditions = [];

                    for ($i = 0; $i < $index; $i++) {
                        $prevKey = $this->keySet->keys[$i];
                        $subConditions[] = $qb->expr()->eq($prevKey->column, ':' . $prevKey->column . '_previous');
                    }
                    $operator = $key->order->value === 'DESC' ? 'lt' : 'gt';
                    $subConditions[] = $qb->expr()->{$operator}($key->column, ':' . $paramName);

                    $conditions[] = $qb->expr()->and(...$subConditions);
                }

                if ($conditions) {
                    $qb->andWhere($qb->expr()->or(...$conditions));

                    foreach ($parameters as $param => $value) {
                        /** @phpstan-ignore-next-line */
                        $qb->setParameter($param, $value, $parameterTypes[$param]);
                    }
                }
            }

            $stmt = $this->connection->executeQuery(
                $qb->getSQL(),
                $qb->getParameters(),
                $qb->getParameterTypes()
            );

            $hasRows = false;

            while ($row = $stmt->fetchAssociative()) {
                $hasRows = true;
                $lastRow = $row;

                $signal = yield array_to_rows($row, $context->entryFactory(), [], $this->schema);

                if ($signal === Extractor\Signal::STOP) {
                    return;
                }

                $totalFetched++;

                if (null !== $this->maximum && $totalFetched >= $this->maximum) {
                    return;
                }
            }

            if (!$hasRows) {
                break;
            }
        }
    }

    /**
     * Sets the maximum number of rows to fetch.
     *
     * @param int $maximum the maximum number of rows (must be > 0)
     *
     * @throws InvalidArgumentException if maximum is <= 0
     *
     * @return $this
     */
    public function withMaximum(int $maximum) : self
    {
        if ($maximum <= 0) {
            throw new InvalidArgumentException('Maximum must be greater than 0, got ' . $maximum);
        }

        $this->maximum = $maximum;

        return $this;
    }

    /**
     * Sets the number of rows per page.
     *
     * @param int $pageSize the page size (must be > 0)
     *
     * @throws InvalidArgumentException if page size is <= 0
     *
     * @return $this
     */
    public function withPageSize(int $pageSize) : self
    {
        if ($pageSize <= 0) {
            throw new InvalidArgumentException('Page size must be greater than 0, got ' . $pageSize);
        }

        $this->pageSize = $pageSize;

        return $this;
    }

    /**
     * Sets the schema for the extracted rows.
     *
     * @param Schema $schema the schema to apply to rows
     *
     * @return $this
     */
    public function withSchema(Schema $schema) : self
    {
        $this->schema = $schema;

        return $this;
    }
}
