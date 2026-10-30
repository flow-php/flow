<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\IO\Extractor;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Flow\ETL\IO\Extractor\ExtractorFactory;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;

use function count;

/**
 * @import-type Params from DriverManager
 */
final class DbalExtractorFactory implements ExtractorFactory
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

    public function from(Rows $rows): DbalQueryExtractor
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

        $extractor = DbalQueryExtractor::single($this->connection(), $this->query);

        if ($this->schema) {
            $extractor->withSchema($this->schema);
        }

        if (count($parameters)) {
            $extractor->withParameters(new ParametersSet($parameters));
        }

        if (count($types)) {
            $extractor->withTypes($types);
        }

        return $extractor;
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
