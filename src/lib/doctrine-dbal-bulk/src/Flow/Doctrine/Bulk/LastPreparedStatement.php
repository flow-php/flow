<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception;
use Doctrine\DBAL\ParameterType;
use Doctrine\DBAL\Statement;
use Doctrine\DBAL\Types\Type;

use function array_is_list;
use function array_key_exists;

/**
 * Consecutive chunks of one shape produce the same SQL, and preparing it again costs a server round trip - on
 * pgsql also a parse of every placeholder - so the last prepared statement is re-bound and re-executed instead.
 */
final class LastPreparedStatement
{
    /**
     * The native handle the statement was prepared on - a reconnect replaces it, and the statement with it.
     *
     * @var null|object|resource
     */
    private mixed $nativeConnection = null;

    private string $sql = '';

    private ?Statement $statement = null;

    /**
     * @param array<string, mixed>|list<mixed> $parameters
     * @param array<non-negative-int|string, ParameterType|string|Type> $types
     *
     * @throws Exception
     */
    public function execute(Connection $connection, string $sql, array $parameters, array $types): void
    {
        // named parameters are rewritten into positional ones by DBAL itself
        if (!array_is_list($parameters)) {
            $connection->executeStatement($sql, $parameters, $types);

            return;
        }

        $nativeConnection = $connection->getNativeConnection();

        if ($this->statement === null || $this->sql !== $sql || $this->nativeConnection !== $nativeConnection) {
            $this->statement = $connection->prepare($sql);
            $this->sql = $sql;
            $this->nativeConnection = $nativeConnection;
        }

        // the binding rule of Connection::executeStatement(): a type keyed like the parameter converts it
        // @mago-ignore analysis:mixed-assignment
        foreach ($parameters as $index => $value) {
            $this->statement->bindValue(
                $index + 1,
                $value,
                array_key_exists($index, $types) ? $types[$index] : ParameterType::STRING,
            );
        }

        $this->statement->executeStatement();
    }
}
