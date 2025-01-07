<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use function Flow\ETL\DSL\df;
use Doctrine\DBAL\{Connection, DriverManager};
use Flow\ETL\{DataFrame, DataFrameFactory, Row\Schema, Rows};

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

    public static function fromConnection(Connection $connection, string $query, QueryParameter ...$parameters) : self
    {
        $factory = new self($connection->getParams(), $query, ...$parameters);
        $factory->connection = $connection;

        return $factory;
    }

    public function from(Rows $rows) : DataFrame
    {
        $parameters = [];
        $types = [];

        foreach ($this->parameters as $parameter) {
            $parameters[$parameter->queryParamName()] = $parameter->toQueryParam($rows);

            if ($parameter->type()) {
                $types[$parameter->queryParamName()] = $parameter->type();
            }
        }

        $extractor = from_dbal_query($this->connection(), $this->query);

        if ($this->schema) {
            $extractor->withSchema($this->schema);
        }

        if (\count($parameters)) {
            $extractor->withParameters(new ParametersSet($parameters));
        }

        if (\count($types)) {
            $extractor->withTypes($types);
        }

        return df()->read($extractor);
    }

    public function withSchema(Schema $schema) : self
    {
        $this->schema = $schema;

        return $this;
    }

    private function connection() : Connection
    {
        if ($this->connection === null) {
            $this->connection = DriverManager::getConnection($this->connectionParams);
        }

        return $this->connection;
    }
}
