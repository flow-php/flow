<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\IO\Extractor;

use Doctrine\DBAL\ArrayParameterType;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\ParameterType;
use Doctrine\DBAL\Types\Type;
use Flow\ETL\Adapter\Doctrine\IO\Extractor\Explain\ExplainedRows;
use Flow\ETL\Foundation\Exception\SchemaNotDerivableException;
use Flow\ETL\IO\Extractor;
use Flow\ETL\IO\Extractor\BatchableExtractor;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\IO\Extractor\Statistics;
use Flow\ETL\Values\BatchSize;
use Flow\ETL\Values\Cardinality;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Rows\RowsBuilder;
use Flow\ETL\Values\Schema;
use Generator;

use function array_slice;
use function count;
use function round;

/**
 * batchSize() caps yields, not fetches: the whole result set materialises per parameter set.
 */
final class DbalQueryExtractor implements BatchableExtractor, Extractor
{
    private const int EXPLAINED_PARAMETER_SETS = 10;

    private BatchSize $batchSize;

    private ParametersSet $parametersSet;

    private ?Schema $schema = null;

    private ?Schema $derived = null;

    private ?SchemaNotDerivableException $refusal = null;

    private ?Statistics $statistics = null;

    /**
     * @var array<int<0, max>|string, ArrayParameterType|ParameterType|string|Type>
     */
    private array $types = [];

    public function __construct(
        private readonly Connection $connection,
        private readonly string $query,
    ) {
        $this->parametersSet = new ParametersSet([]);
        $this->batchSize = new BatchSize();
    }

    /**
     * @return int<1, max>
     */
    public function batchSize(): int
    {
        return $this->batchSize->size;
    }

    public function isRepeatable(): bool
    {
        return true;
    }

    /**
     * @param array<string, mixed>|list<mixed> $parameters
     * @param array<int<0, max>|string, ArrayParameterType|ParameterType|string|Type> $types
     */
    public static function single(
        Connection $connection,
        string $query,
        array $parameters = [],
        array $types = [],
    ): self {
        $extractor = new self($connection, $query);

        if ($parameters !== []) {
            $extractor->withParameters(new ParametersSet($parameters));
        }

        if ($types !== []) {
            $extractor->withTypes($types);
        }

        return $extractor;
    }

    /**
     * @return Generator<int, Rows, Signal|null, void>
     */
    public function extract(Backend $backend, Criteria $criteria): Generator
    {
        $schema = $this->schema();
        $yielded = 0;

        foreach ($this->parametersSet->all() as $parameters) {
            if ($criteria->limit !== null && $yielded >= $criteria->limit) {
                return;
            }

            $all = (new RowsBuilder($schema, $backend))
                ->appendRows($this->connection->fetchAllAssociative($this->query, $parameters, $this->types))
                ->finish();

            foreach ($all->chunks($this->batchSize->size) as $batch) {
                $yielded += $batch->count();

                $signal = yield $batch;

                if ($signal === Signal::STOP) {
                    return;
                }

                if (
                    $batch->count() === $this->batchSize->size
                    && $criteria->limit !== null
                    && $yielded >= $criteria->limit
                ) {
                    return;
                }
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
            // The SQL - and so the result shape - is identical for every parameter set, so the probe binds none of them.
            return $this->derived ??= (new DbalResultSchema())->of($this->connection, $this->query, self::class);
        } catch (SchemaNotDerivableException $refusal) {
            $this->refusal = $refusal;

            throw $refusal;
        }
    }

    /**
     * One plan per parameter set, summed; past EXPLAINED_PARAMETER_SETS the explained sets are scaled to all of them,
     * so planning never pays a round trip per set.
     */
    public function statistics(): Statistics
    {
        if ($this->statistics === null) {
            $sets = $this->parametersSet->all();
            $explained = array_slice($sets, 0, self::EXPLAINED_PARAMETER_SETS);
            $rows = Cardinality::exact(0);

            foreach ($explained as $parameters) {
                $rows = $rows->merge((new ExplainedRows())->of(
                    $this->connection,
                    $this->query,
                    $parameters,
                    $this->types,
                ));
            }

            $this->statistics = new Statistics(
                rows: count($sets) === count($explained)
                    ? $rows
                    : new Cardinality(
                        estimate: $rows->estimate === null
                            ? null
                            : (int) round(($rows->estimate * count($sets)) / count($explained)),
                        relativeError: Cardinality::DEFAULT_RELATIVE_ERROR,
                    ),
            );
        }

        return $this->statistics;
    }

    public function withBatchSize(int $batchSize): static
    {
        $this->batchSize = new BatchSize($batchSize);

        return $this;
    }

    public function withParameters(ParametersSet $parametersSet): self
    {
        $this->parametersSet = $parametersSet;
        $this->statistics = null;

        return $this;
    }

    public function withSchema(Schema $schema): static
    {
        $this->schema = $schema;

        return $this;
    }

    /**
     * @param array<int<0, max>|string, ArrayParameterType|ParameterType|string|Type> $types
     */
    public function withTypes(array $types): self
    {
        $this->types = $types;
        $this->statistics = null;

        return $this;
    }
}
