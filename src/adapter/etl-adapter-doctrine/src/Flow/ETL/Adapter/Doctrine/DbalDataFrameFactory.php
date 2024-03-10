<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use Doctrine\DBAL\{Connection, DriverManager};
use Flow\ETL\{DataFrame, DataFrameFactory, Flow, Rows};

final class DbalDataFrameFactory implements DataFrameFactory
{
    private ?Connection $connection = null;

    /**
     * @var array<QueryParameter>
     */
    private array $parameters;

    /**
     * @param array<string, mixed> $connectionParams
     * @param string $query
     * @param QueryParameter ...$parameters
     */
    public function __construct(
        private readonly array $connectionParams,
        private readonly string $query,
        QueryParameter ...$parameters
    ) {
        $this->parameters = $parameters;
    }

    public static function fromConnection(Connection $connection, string $query, QueryParameter ...$parameters) : self
    {
        /** @psalm-suppress InternalMethod */
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

        /** @psalm-suppress InvalidArgument */
        return (new Flow())->extract(\Flow\ETL\Adapter\Doctrine\dbal_from_query($this->connection(), $this->query, $parameters, $types));
    }

    private function connection() : Connection
    {
        if ($this->connection === null) {
            /**
             * @psalm-suppress ArgumentTypeCoercion
             */
            $this->connection = DriverManager::getConnection($this->connectionParams);
        }

        return $this->connection;
    }
}
