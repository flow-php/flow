<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Query\QueryBuilder;
use Flow\ETL\Adapter\Doctrine\Explain\ExplainedRows;
use Flow\ETL\Adapter\Doctrine\Pagination\Key;
use Flow\ETL\Adapter\Doctrine\Pagination\KeySet;
use Flow\ETL\Adapter\Doctrine\Pagination\KeySetPage;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\BatchableExtractor;
use Flow\ETL\Extractor\Batches;
use Flow\ETL\Extractor\RewindableExtractor;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\Extractor\Statistics;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Rows\RowsBuilder;
use Flow\ETL\Schema;
use Generator;

use function array_map;
use function implode;
use function min;
use function sha1;

/**
 * Keyset pagination over a Doctrine DBAL query; the keys must be indexed, non-null and unique together, and the query
 * must carry no ORDER BY, setMaxResults() or setFirstResult().
 */
final class DbalKeySetExtractor implements BatchableExtractor, Extractor, RewindableExtractor
{
    use Batches;

    private string $keyAliasSuffix = '_previous';

    private ?int $maximum = null;

    private ?Schema $schema = null;

    private ?Schema $derived = null;

    private ?SchemaNotDerivableException $refusal = null;

    private ?Statistics $statistics = null;

    public function __construct(
        private readonly Connection $connection,
        private readonly QueryBuilder $queryBuilder,
        private readonly KeySet $keySet,
    ) {
        $cleanQuery = (clone $this->queryBuilder)->resetOrderBy();

        if ($cleanQuery->getSQL() !== $this->queryBuilder->getSQL()) {
            throw new InvalidArgumentException(
                'Keyset pagination cannot be used with an ORDER BY clause, please remove OrderBy from Query Builder',
            );
        }

        if ($this->queryBuilder->getMaxResults() !== null || $this->queryBuilder->getFirstResult() !== 0) {
            throw new InvalidArgumentException(
                'Keyset pagination sets its own LIMIT and OFFSET, please remove setMaxResults()/setFirstResult() '
                . 'from Query Builder and use withMaximum() or DataFrame::limit()',
            );
        }

        // a page is a network round trip, not a buffer: 100 would cost 10x the round trips
        $this->batchSize = 1_000;
    }

    public function isRepeatable(): bool
    {
        return true;
    }

    /**
     * @return Generator<int, Rows, Signal|null, void>
     */
    public function extract(FlowContext $context, ?int $limit = null): Generator
    {
        $schema = $this->schema();
        $yielded = 0;
        $maximum = match (true) {
            $this->maximum !== null && $limit !== null => min($this->maximum, $limit),
            $this->maximum !== null => $this->maximum,
            default => $limit,
        };
        $keyAliases = array_map($this->keyAlias(...), $this->keySet->keys);

        // DBAL's QueryBuilder is mutable and has no copy API other than its deep __clone()
        $check = clone $this->queryBuilder;
        $check
            ->andWhere($check->expr()->or(...array_map(static fn(Key $key): string => $check
                ->expr()
                ->isNull($key->column), $this->keySet->keys)))
            ->setMaxResults(1);

        if (
            $this->connection->executeQuery(
                $check->getSQL(),
                $this->queryBuilder->getParameters(),
                $this->queryBuilder->getParameterTypes(),
            )->fetchAssociative() !== false
        ) {
            throw new RuntimeException(sprintf(
                'Keyset pagination requires non-null keys, but a row has NULL in the key column(s) %s; filter them out with IS NOT NULL or choose non-null keys',
                implode(', ', array_map(static fn(Key $key): string => '"' . $key->column . '"', $this->keySet->keys)),
            ));
        }

        $cursor = null;
        $expected = null;

        while (true) {
            if ($maximum !== null && $yielded >= $maximum) {
                return;
            }

            $size = $maximum === null ? $this->batchSize : min($this->batchSize, $maximum - $yielded);

            $qb = clone $this->queryBuilder;
            $qb->setMaxResults($size + 1);

            foreach ($this->keySet->keys as $index => $key) {
                $qb->addOrderBy($key->column, $key->order->value);
                $qb->addSelect($key->column . ' AS ' . $keyAliases[$index]);
            }

            if ($cursor !== null) {
                $conditions = [];

                foreach ($this->keySet->keys as $index => $key) {
                    $subConditions = [];

                    for ($i = 0; $i < $index; $i++) {
                        $subConditions[] = $qb->expr()->eq($this->keySet->keys[$i]->column, ':' . $keyAliases[$i]);
                    }

                    $operator = $key->order->value === 'DESC' ? 'lt' : 'gt';
                    $subConditions[] = $qb->expr()->{$operator}($key->column, ':' . $keyAliases[$index]);

                    $conditions[] = $qb->expr()->and(...$subConditions);

                    $qb->setParameter($keyAliases[$index], $cursor->values[$index], $key->type);
                }

                $qb->andWhere($qb->expr()->or(...$conditions));
            }

            // the pgsql driver resolves the result's column types on every fetchAssociative(), but once per
            // fetchAllAssociative(); a page is buffered whole either way
            $page = KeySetPage::of(
                $this->keySet,
                $keyAliases,
                $this->connection
                    ->executeQuery($qb->getSQL(), $qb->getParameters(), $qb->getParameterTypes())
                    ->fetchAllAssociative(),
                $size,
                $expected,
            );

            if ($page->rows !== []) {
                $rawBatch = [];

                foreach ($page->rows as $row) {
                    foreach ($keyAliases as $keyAlias) {
                        unset($row[$keyAlias]);
                    }

                    $rawBatch[] = $row;
                }

                $rows = (new RowsBuilder($schema, $context->backend()))
                    ->appendRows($rawBatch)
                    ->finish();

                $yielded += $rows->count();

                $signal = yield $rows;

                if ($signal === Signal::STOP) {
                    return;
                }
            }

            if ($page->isLast()) {
                return;
            }

            $cursor = $page->cursor;
            $expected = $page->lookahead;
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
            // The base builder, never the page SQL: the key_<sha1> alias lives on a per-page clone.
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

    public function statistics(): Statistics
    {
        return $this->statistics ??= new Statistics(rows: (new ExplainedRows())->of(
            $this->connection,
            $this->queryBuilder->getSQL(),
            $this->queryBuilder->getParameters(),
            $this->queryBuilder->getParameterTypes(),
            $this->maximum,
        ));
    }

    public function withKeyAliasSuffix(string $keyAliasSuffix): self
    {
        $this->keyAliasSuffix = $keyAliasSuffix;

        return $this;
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
    public function withMaximum(int $maximum): self
    {
        if ($maximum <= 0) {
            throw new InvalidArgumentException('Maximum must be greater than 0, got ' . $maximum);
        }

        $this->maximum = $maximum;
        $this->statistics = null;

        return $this;
    }

    /**
     * Sets the schema for the extracted rows.
     *
     * @param Schema $schema the schema to apply to rows
     *
     * @return $this
     */
    public function withSchema(Schema $schema): static
    {
        $this->schema = $schema;

        return $this;
    }

    private function keyAlias(Key $key): string
    {
        return 'key_'
        . sha1((string) preg_replace(
            '/[^a-zA-Z0-9_]/',
            '_',
            str_replace('.', '_', $key->column . $this->keyAliasSuffix),
        ));
    }
}
