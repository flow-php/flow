<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use Doctrine\DBAL\ArrayParameterType;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\ParameterType;
use Doctrine\DBAL\Types\Type;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\RewindableExtractor;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Generator;

final class DbalQueryExtractor implements Extractor, RewindableExtractor
{
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

        foreach ($this->parametersSet->all() as $parameters) {
            $rawBatch = [];

            foreach ($this->connection->fetchAllAssociative($this->query, $parameters, $this->types) as $row) {
                $rawBatch[] = $row;
            }

            $hydrated = $hydrator->hydrate($encoder->decode($rawBatch), $schema);

            foreach ($hydrated as $hydratedRow) {
                $signal = yield Rows::trusted($hydrated->schema(), [$hydratedRow]);

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
