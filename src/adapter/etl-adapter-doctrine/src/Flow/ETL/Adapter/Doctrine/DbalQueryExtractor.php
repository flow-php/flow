<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use function Flow\ETL\DSL\array_to_rows;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Types\Type;
use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;

final class DbalQueryExtractor implements Extractor
{
    /**
     * @var ParametersSet
     */
    private readonly ParametersSet $parametersSet;

    /**
     * @param null|ParametersSet $parametersSet
     * @param array<int, null|int|string|Type>|array<string, null|int|string|Type> $types
     */
    public function __construct(
        private readonly Connection $connection,
        private readonly string $query,
        ?ParametersSet $parametersSet = null,
        private readonly array $types = [],
    ) {
        $this->parametersSet = $parametersSet ?: new ParametersSet([]);
    }

    /**
     * @param array<string, mixed>|list<mixed> $parameters
     * @param array<int, null|int|string|Type>|array<string, null|int|string|Type> $types
     */
    public static function single(Connection $connection, string $query, array $parameters = [], array $types = []) : self
    {
        return new self($connection, $query, new ParametersSet($parameters), $types);
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
}
