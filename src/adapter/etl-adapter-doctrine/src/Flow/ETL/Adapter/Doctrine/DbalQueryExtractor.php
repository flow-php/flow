<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use Doctrine\DBAL\ArrayParameterType;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\ParameterType;
use Doctrine\DBAL\Types\Type;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Generator;

final class DbalQueryExtractor implements Extractor
{
    private ParametersSet $parametersSet;

    private ?Schema $schema = null;

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
        $hydrator = $context->hydrator();
        $encoder = new DbalEncoder();

        foreach ($this->parametersSet->all() as $parameters) {
            $rawBatch = [];

            foreach ($this->connection->fetchAllAssociative($this->query, $parameters, $this->types) as $row) {
                $rawBatch[] = $row;
            }

            $hydrated = $hydrator->cast($encoder->decode($rawBatch), $this->schema);

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
        if ($this->schema === null) {
            throw SchemaNotDerivableException::extractor(self::class);
        }

        return $this->schema;
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
