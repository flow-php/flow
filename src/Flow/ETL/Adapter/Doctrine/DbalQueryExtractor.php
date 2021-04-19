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
     * @var array<string, mixed>|list<mixed>
     */
    private array $parameters;

    /**
     * @var array<int, null|int|string|Type>|array<string, null|int|string|Type>
     */
    private array $types;

    private string $rowEntryName;

    /**
     * DbalQueryExtractor constructor.
     *
     * @param Connection $connection
     * @param string $query
     * @param array<string, mixed>|list<mixed> $parameters
     * @param array<int, null|int|string|Type>|array<string, null|int|string|Type> $types
     * @param string $rowEntryName
     */
    public function __construct(Connection $connection, string $query, array $parameters = [], array $types = [], string $rowEntryName = 'row')
    {
        $this->connection = $connection;
        $this->query = $query;
        $this->parameters = $parameters;
        $this->types = $types;
        $this->rowEntryName = $rowEntryName;
    }

    public function extract() : \Generator
    {
        $rows = new Rows();

        /** @psalm-suppress ImpureMethodCall */
        foreach ($this->connection->fetchAllAssociative($this->query, $this->parameters, $this->types) as $row) {
            $rows = $rows->add(Row::create(new Row\Entry\ArrayEntry($this->rowEntryName, $row)));
        }

        yield $rows;
    }
}
