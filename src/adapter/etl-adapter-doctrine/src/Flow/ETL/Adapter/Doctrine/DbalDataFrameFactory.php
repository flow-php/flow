<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Flow\ETL\DataFrame;
use Flow\ETL\DataFrameFactory;
use Flow\ETL\Rows;
use Flow\ETL\Schema;

use function count;
use function Flow\ETL\DSL\df;

/**
 * @import-type Params from DriverManager
 */
final class DbalDataFrameFactory implements DataFrameFactory
{
    private ?Connection $connection = null;

    /**
     * @var array<QueryParameter>
     */
    private readonly array $parameters;

    private ?Schema $schema = null;

    /**
     * @param array<string, mixed> $connectionParams
     * @param string $query
     * @param QueryParameter ...$parameters
     */
    public function __construct(
        private readonly array $connectionParams,
        private readonly string $query,
        QueryParameter ...$parameters,
    ) {
        $this->parameters = $parameters;
    }

    public static function fromConnection(Connection $connection, string $query, QueryParameter ...$parameters): self
    {
        $factory = new self($connection->getParams(), $query, ...$parameters);
        $factory->connection = $connection;

        return $factory;
    }

    public function from(Rows $rows): DataFrame
    {
        $parameters = [];
        $types = [];

        foreach ($this->parameters as $parameter) {
            $parameters[$parameter->queryParamName()] = $parameter->toQueryParam($rows);

            $type = $parameter->type();

            if ($type !== null) {
                $types[$parameter->queryParamName()] = $type;
            }
        }

        $extractor = from_dbal_query($this->connection(), $this->query);

        if ($this->schema) {
            $extractor->withSchema($this->schema);
        }

        if (count($parameters)) {
            $extractor->withParameters(new ParametersSet($parameters));
        }

        if (count($types)) {
            $extractor->withTypes($types);
        }

        return df()->read($extractor);
    }

    /**
     * @param Schema $schema
     */
    public function withSchema(Schema $schema): static
    {
        $this->schema = $schema;

        return $this;
    }

    private function connection(): Connection
    {
        if ($this->connection === null) {
            /** @var Params $connectionParams */
            $connectionParams = $this->connectionParams;
            $this->connection = DriverManager::getConnection($connectionParams);
        }

        return $this->connection;
    }
}
