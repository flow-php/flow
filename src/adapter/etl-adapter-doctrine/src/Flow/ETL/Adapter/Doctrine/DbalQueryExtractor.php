<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use function Flow\ETL\DSL\array_to_rows;
use Doctrine\DBAL\Types\Type;
use Doctrine\DBAL\{ArrayParameterType, Connection, ParameterType};
use Flow\ETL\{Extractor, FlowContext};

final class DbalQueryExtractor implements Extractor
{
    private ParametersSet $parametersSet;

    private array $types = [];

    public function __construct(
        private readonly Connection $connection,
        private readonly string $query,
    ) {
        $this->parametersSet = new ParametersSet([]);
    }

    /**
     * @param array<string, mixed>|list<mixed> $parameters
     * @param array<int|string, ArrayParameterType|int|ParameterType|string|Type> $types
     */
    public static function single(Connection $connection, string $query, array $parameters = [], array $types = []) : self
    {
        return (new self($connection, $query))->withParameters(new ParametersSet([$parameters]))->withTypes($types);
    }

    public function extract(FlowContext $context) : \Generator
    {
        foreach ($this->parametersSet->all() as $parameters) {
            foreach ($this->connection->fetchAllAssociative($this->query, $parameters, $this->types) as $row) {
                $signal = yield array_to_rows($row, $context->entryFactory());

                if ($signal === Extractor\Signal::STOP) {
                    return;
                }
            }
        }
    }

    public function withParameters(ParametersSet $parametersSet) : self
    {
        $this->parametersSet = $parametersSet;

        return $this;
    }

    /**
     * @param array<int|string, ArrayParameterType|int|ParameterType|string|Type> $types
     */
    public function withTypes(array $types) : self
    {
        $this->types = $types;

        return $this;
    }
}
