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
    private Connection $connection;

    private string $query;

    /**
     * @var ParametersSet
     */
    private ParametersSet $parametersSet;

    /**
     * @var array<int, null|int|string|Type>|array<string, null|int|string|Type>
     */
    private array $types;

    private string $rowEntryName;

    /**
     * @param Connection $connection
     * @param string $query
     * @param null|ParametersSet $parametersSet
     * @param array<int, null|int|string|Type>|array<string, null|int|string|Type> $types
     * @param string $rowEntryName
     */
    public function __construct(Connection $connection, string $query, ParametersSet $parametersSet = null, array $types = [], string $rowEntryName = 'row')
    {
        $this->connection = $connection;
        $this->query = $query;
        $this->types = $types;
        $this->rowEntryName = $rowEntryName;
        $this->parametersSet = $parametersSet ? $parametersSet : new ParametersSet([]);
    }

    /**
     * @param Connection $connection
     * @param string $query
     * @param array<string, mixed>|list<mixed> $parameters
     * @param array<int, null|int|string|Type>|array<string, null|int|string|Type> $types
     * @param string $rowEntryName
     *
     * @return DbalQueryExtractor
     */
    public static function single(Connection $connection, string $query, array $parameters = [], array $types = [], string $rowEntryName = 'row') : self
    {
        return new self($connection, $query, new ParametersSet($parameters), $types, $rowEntryName);
    }

    public function extract() : \Generator
    {
        foreach ($this->parametersSet->all() as $parameters) {
            $rows = new Rows();

            /** @psalm-suppress ImpureMethodCall */
            foreach ($this->connection->fetchAllAssociative($this->query, $parameters, $this->types) as $row) {
                $rows = $rows->add(Row::create(new Row\Entry\ArrayEntry($this->rowEntryName, $row)));
            }

            yield $rows;
        }
    }
}
