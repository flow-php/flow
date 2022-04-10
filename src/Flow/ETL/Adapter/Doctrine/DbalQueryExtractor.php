<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Types\Type;
use Flow\ETL\Extractor;
use Flow\ETL\Row;
use Flow\ETL\Rows;

/**
 * @psalm-immutable
 */
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
    public function __construct(private readonly Connection $connection, private readonly string $query, ParametersSet $parametersSet = null, private readonly array $types = [], private readonly string $rowEntryName = 'row')
    {
        $this->parametersSet = $parametersSet ?: new ParametersSet([]);
    }

    /**
     * @param array<string, mixed>|list<mixed> $parameters
     * @param array<int, null|int|string|Type>|array<string, null|int|string|Type> $types
     */
    public static function single(Connection $connection, string $query, array $parameters = [], array $types = [], string $rowEntryName = 'row') : self
    {
        return new self($connection, $query, new ParametersSet($parameters), $types, $rowEntryName);
    }

    public function extract() : \Generator
    {
        foreach ($this->parametersSet->all() as $parameters) {
            $rows = [];

            /** @psalm-suppress ImpureMethodCall */
            foreach ($this->connection->fetchAllAssociative($this->query, $parameters, $this->types) as $row) {
                $rows[] = Row::create(new Row\Entry\ArrayEntry($this->rowEntryName, $row));
            }

            yield new Rows(...$rows);
        }
    }
}
