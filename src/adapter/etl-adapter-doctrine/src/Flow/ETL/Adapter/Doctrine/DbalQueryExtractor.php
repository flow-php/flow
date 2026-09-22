<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use Doctrine\DBAL\ArrayParameterType;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\ParameterType;
use Doctrine\DBAL\Types\Type;
use Flow\ETL\Adapter\Doctrine\Explain\ExplainedRows;
use Flow\ETL\Cardinality;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\BatchableExtractor;
use Flow\ETL\Extractor\Batches;
use Flow\ETL\Extractor\RewindableExtractor;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\Extractor\Statistics;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Generator;

use function array_slice;
use function count;
use function round;

/**
 * batchSize() is a yield cap, not a fetch size: fetchAllAssociative() materialises the whole result set per
 * parameter set, so lowering it cannot lower peak memory. The paginating extractors default to 1000 because
 * their number IS a round trip.
 *
 * A pushed limit is global across parameter sets - their batches are concatenated - and no set is queried
 * once it is reached. The query is a raw string, so the first queried set is never bounded server-side.
 */
final class DbalQueryExtractor implements BatchableExtractor, Extractor, RewindableExtractor
{
    private const int EXPLAINED_PARAMETER_SETS = 10;

    use Batches;

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
    public function extract(FlowContext $context, ?int $limit = null): Generator
    {
        $schema = $this->schema();
        $hydrator = $context->hydrator();
        $encoder = new DbalEncoder();
        $yielded = 0;

        foreach ($this->parametersSet->all() as $parameters) {
            if ($limit !== null && $yielded >= $limit) {
                return;
            }

            $rawBatch = [];

            foreach ($this->connection->fetchAllAssociative($this->query, $parameters, $this->types) as $row) {
                $rawBatch[] = $row;
            }

            $hydrated = $hydrator->hydrate($encoder->decode($rawBatch), $schema);
            $buffer = [];

            foreach ($hydrated as $hydratedRow) {
                $buffer[] = $hydratedRow;
                $yielded++;

                if (count($buffer) === $this->batchSize) {
                    $signal = yield Rows::trusted($hydrated->schema(), $buffer);

                    if ($signal === Signal::STOP) {
                        return;
                    }

                    $buffer = [];

                    if ($limit !== null && $yielded >= $limit) {
                        return;
                    }
                }
            }

            if ($buffer !== []) {
                $signal = yield Rows::trusted($hydrated->schema(), $buffer);

                if ($signal === Signal::STOP) {
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
