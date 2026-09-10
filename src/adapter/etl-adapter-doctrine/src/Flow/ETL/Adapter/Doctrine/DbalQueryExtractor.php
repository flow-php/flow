<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use Doctrine\DBAL\ArrayParameterType;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\ParameterType;
use Doctrine\DBAL\Types\Type;
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

/**
 * batchSize() is a yield cap, not a fetch size: fetchAllAssociative() materialises the whole result set per
 * parameter set, so lowering it cannot lower peak memory. The paginating extractors default to 1000 because
 * their number IS a round trip.
 *
 * A pushed limit is global across parameter sets - their batches are concatenated - and no set is queried
 * once it is reached. The query is a raw string, so the first queried set is never bounded server-side.
 */
final class DbalQueryExtractor implements BatchableExtractor, Extractor, LimitPushDown, RewindableExtractor
{
    use Batches;
    use PushesLimit;

    private ParametersSet $parametersSet;

    private ?Schema $schema = null;

    private ?Schema $derived = null;

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
    public function extract(FlowContext $context): Generator
    {
        $schema = $this->schema();
        $hydrator = $context->hydrator();
        $encoder = new DbalEncoder();
        $yielded = 0;
        $maximum = $this->pushedLimit();

        foreach ($this->parametersSet->all() as $parameters) {
            if ($maximum !== null && $yielded >= $maximum) {
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

                    if ($maximum !== null && $yielded >= $maximum) {
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
        // The SQL - and so the result shape - is identical for every parameter set, so the first
        // one is a representative binding; its values are nulled by the probe anyway.
        return (
            $this->schema ?? ($this->derived ??= (new DbalResultSchema())->of(
                $this->connection,
                $this->query,
                self::class,
            ))
        );
    }

    public function withParameters(ParametersSet $parametersSet): self
    {
        $this->parametersSet = $parametersSet;

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

        return $this;
    }
}
