<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Infrastructure\PgSql;

use function Flow\PostgreSql\DSL\{begin, commit, release_savepoint, rollback, savepoint};
use function Flow\Types\DSL\{type_boolean, type_datetime, type_float, type_integer, type_json, type_list, type_string, type_uuid};
use Flow\PostgreSql\AST\Transformers\{ExplainConfig, ExplainModifier};
use Flow\PostgreSql\Client\{Client, ConnectionParameters, Cursor, RowMapper, TransactionContext, TypedValue};
use Flow\PostgreSql\Client\Exception\{ConnectionException, MappingException, QueryException, TransactionException};
use Flow\PostgreSql\Client\Types\{PostgreSqlType, ValueConverter, ValueConverters};
use Flow\PostgreSql\Explain\ExplainParser;
use Flow\PostgreSql\Explain\Plan\Plan;
use Flow\PostgreSql\Parser;
use Flow\PostgreSql\QueryBuilder\SqlQuery;
use Flow\Types\Value\{Json, Uuid};
use PgSql\{Connection, Result};

final class PgSqlClient implements Client
{
    private bool $autoCommit = true;

    private readonly TransactionContext $transactionContext;

    private function __construct(private ?Connection $connection, private readonly ValueConverters $valueConverters, private readonly ?RowMapper $defaultMapper = null)
    {
        $this->transactionContext = new TransactionContext();
    }

    /**
     * Connect using ConnectionParameters.
     */
    public static function connect(
        ConnectionParameters $params,
        ?ValueConverters $valueConverters = null,
        ?RowMapper $mapper = null,
    ) : self {
        self::assertExtensionLoaded();

        \error_clear_last();
        $connection = @\pg_connect($params->connectionString);

        if ($connection === false) {
            $error = \error_get_last();

            throw ConnectionException::connectionFailed($error['message'] ?? 'Unknown error');
        }

        return new self(
            $connection,
            $valueConverters ?? ValueConverters::create(),
            $mapper,
        );
    }

    public function beginTransaction() : void
    {
        $this->assertConnected();

        $savepointName = $this->transactionContext->begin();

        if ($savepointName === null) {
            $this->executeTransactionCommand(begin(), TransactionException::beginFailed(...));
        } else {
            $this->executeTransactionCommand(
                savepoint($savepointName),
                fn (string $error) => TransactionException::savepointFailed($savepointName, $error)
            );
        }
    }

    public function close() : void
    {
        if ($this->connection !== null) {
            @\pg_close($this->connection);
            $this->connection = null;
        }
    }

    public function commit() : void
    {
        $this->assertConnected();

        $savepointName = $this->transactionContext->commit();

        if ($savepointName === null) {
            $this->executeTransactionCommand(commit(), TransactionException::commitFailed(...));
        } else {
            $this->executeTransactionCommand(
                release_savepoint($savepointName),
                fn (string $error) => TransactionException::releaseSavepointFailed($savepointName, $error)
            );
        }
    }

    public function cursor(SqlQuery|string $sql, array $parameters = []) : Cursor
    {
        $result = $this->query($sql, $parameters);

        return new PgSqlCursor($result, $this->valueConverters, $this->defaultMapper);
    }

    public function execute(SqlQuery|string $sql, array $parameters = []) : int
    {
        $result = $this->query($sql, $parameters);
        $affected = \pg_affected_rows($result);
        \pg_free_result($result);

        return $affected;
    }

    public function explain(SqlQuery|string $sql, array $parameters = [], ?ExplainConfig $config = null) : Plan
    {
        $config ??= ExplainConfig::forAnalysis();
        $query = $sql instanceof SqlQuery ? $sql->toSql() : $sql;

        $parsed = (new Parser())->parse($query);
        $parsed->traverse(new ExplainModifier($config));
        $explainQuery = $parsed->deparse();

        $jsonOutput = $this->fetchScalar($explainQuery, $parameters);

        if ($jsonOutput instanceof Json) {
            $jsonOutput = $jsonOutput->toString();
        }

        /** @var string $jsonOutput */
        return (new ExplainParser())->parse($jsonOutput);
    }

    public function fetch(SqlQuery|string $sql, array $parameters = []) : ?array
    {
        $result = $this->query($sql, $parameters);
        $row = \pg_fetch_assoc($result);

        if ($row === false) {
            \pg_free_result($result);

            return null;
        }

        $converted = $this->convertRow($result, $row);
        \pg_free_result($result);

        return $converted;
    }

    public function fetchAll(SqlQuery|string $sql, array $parameters = []) : array
    {
        $result = $this->query($sql, $parameters);
        $rows = \pg_fetch_all($result) ?: [];

        if ($rows !== []) {
            $rows = \array_map(
                fn (array $row) => $this->convertRow($result, $row),
                $rows
            );
        }

        \pg_free_result($result);

        return $rows;
    }

    public function fetchAllInto(
        string $class,
        SqlQuery|string $sql,
        array $parameters = [],
        ?RowMapper $mapper = null,
    ) : array {
        $rows = $this->fetchAll($sql, $parameters);

        if ($rows === []) {
            return [];
        }

        $mapper = $this->resolveMapper($mapper);

        return \array_map(
            fn (array $row) => $mapper->map($class, $row),
            $rows,
        );
    }

    public function fetchInto(
        string $class,
        SqlQuery|string $sql,
        array $parameters = [],
        ?RowMapper $mapper = null,
    ) : ?object {
        $row = $this->fetch($sql, $parameters);

        if ($row === null) {
            return null;
        }

        return $this->resolveMapper($mapper)->map($class, $row);
    }

    public function fetchOne(SqlQuery|string $sql, array $parameters = []) : array
    {
        $result = $this->query($sql, $parameters);
        $count = \pg_num_rows($result);

        if ($count === 0) {
            \pg_free_result($result);

            throw QueryException::noRowsFound();
        }

        if ($count > 1) {
            \pg_free_result($result);

            throw QueryException::tooManyRows($count);
        }

        $row = \pg_fetch_assoc($result);

        if ($row === false) {
            \pg_free_result($result);

            throw QueryException::noRowsFound();
        }

        $converted = $this->convertRow($result, $row);
        \pg_free_result($result);

        return $converted;
    }

    public function fetchOneInto(
        string $class,
        SqlQuery|string $sql,
        array $parameters = [],
        ?RowMapper $mapper = null,
    ) : object {
        $row = $this->fetchOne($sql, $parameters);

        return $this->resolveMapper($mapper)->map($class, $row);
    }

    public function fetchScalar(SqlQuery|string $sql, array $parameters = []) : mixed
    {
        $result = $this->query($sql, $parameters);

        if (\pg_num_rows($result) === 0) {
            \pg_free_result($result);

            return null;
        }

        $value = \pg_fetch_result($result, 0, 0);

        if ($value === false) {
            \pg_free_result($result);

            return null;
        }

        if ($value !== null) {
            $oid = \pg_field_type_oid($result, 0);
            $type = PostgreSqlType::tryFrom($oid);

            if ($type !== null) {
                $value = $this->valueConverters->forPostgreSqlType($type)->toPhp($value, $type);
            }
        }

        \pg_free_result($result);

        return $value;
    }

    public function fetchScalarBool(SqlQuery|string $sql, array $parameters = []) : bool
    {
        return type_boolean()->assert($this->fetchScalar($sql, $parameters));
    }

    public function fetchScalarFloat(SqlQuery|string $sql, array $parameters = []) : float
    {
        return type_float()->assert($this->fetchScalar($sql, $parameters));
    }

    public function fetchScalarInt(SqlQuery|string $sql, array $parameters = []) : int
    {
        return type_integer()->assert($this->fetchScalar($sql, $parameters));
    }

    public function fetchScalarString(SqlQuery|string $sql, array $parameters = []) : string
    {
        return type_string()->assert($this->fetchScalar($sql, $parameters));
    }

    public function getTransactionNestingLevel() : int
    {
        return $this->transactionContext->getNestingLevel();
    }

    public function isAutoCommit() : bool
    {
        return $this->autoCommit;
    }

    public function isConnected() : bool
    {
        return $this->connection !== null
            && \pg_connection_status($this->connection) === \PGSQL_CONNECTION_OK;
    }

    public function lastInsertId(string $sequenceName) : int|string
    {
        try {
            $result = $this->fetchScalar('SELECT currval($1)', [$sequenceName]);
        } catch (QueryException $e) {
            if (\str_contains($e->getMessage(), 'is not yet defined in this session')) {
                throw QueryException::sequenceNotUsed($sequenceName);
            }

            throw $e;
        }

        if ($result === null) {
            throw QueryException::sequenceNotUsed($sequenceName);
        }

        /** @var int|string $result */
        return $result;
    }

    public function rollBack() : void
    {
        $this->assertConnected();

        $savepointName = $this->transactionContext->rollBack();

        /** @var Connection $connection */
        $connection = $this->connection;

        if ($savepointName === null) {
            @\pg_query($connection, rollback()->toSql());
        } else {
            @\pg_query($connection, rollback()->toSavepoint($savepointName)->toSql());
        }
    }

    public function setAutoCommit(bool $autoCommit) : void
    {
        if ($this->autoCommit === $autoCommit) {
            return;
        }

        $this->autoCommit = $autoCommit;

        if (!$autoCommit && $this->transactionContext->getNestingLevel() === 0) {
            $this->beginTransaction();
        }

        if ($autoCommit && $this->transactionContext->getNestingLevel() === 1) {
            $this->commit();
        }
    }

    public function transaction(callable $callback) : mixed
    {
        $this->beginTransaction();

        try {
            $returnValue = $callback($this);
            $this->commit();

            return $returnValue;
        } catch (\Throwable $e) {
            $this->rollBack();

            throw $e;
        }
    }

    private function assertConnected() : void
    {
        if (!$this->isConnected()) {
            throw ConnectionException::notConnected();
        }
    }

    /**
     * @param array<int, mixed> $parameters
     *
     * @return array<int, null|string>
     */
    private function convertParameters(array $parameters) : array
    {
        $converted = [];

        foreach ($parameters as $value) {
            if ($value === null) {
                $converted[] = null;
            } elseif ($value instanceof TypedValue) {
                $converter = $this->valueConverters->forFlowType($value->type);
                $converted[] = $converter->toDatabase($value->value);
            } else {
                $converter = $this->detectConverterForValue($value);
                $converted[] = $converter->toDatabase($value);
            }
        }

        return $converted;
    }

    /**
     * @param array<int|string, null|string> $row
     *
     * @return array<string, mixed>
     */
    private function convertRow(Result $result, array $row) : array
    {
        $converted = [];
        $i = 0;

        foreach ($row as $column => $value) {
            if ($value === null) {
                $converted[$column] = null;
            } else {
                $oid = \pg_field_type_oid($result, $i);
                $type = PostgreSqlType::tryFrom($oid);
                $converted[$column] = $type !== null
                    ? $this->valueConverters->forPostgreSqlType($type)->toPhp($value, $type)
                    : $value;
            }

            $i++;
        }

        return $converted;
    }

    /**
     * @return ValueConverter<mixed>
     */
    private function detectConverterForValue(mixed $value) : ValueConverter
    {
        return match (true) {
            \is_bool($value) => $this->valueConverters->forFlowType(type_boolean()),
            \is_int($value) => $this->valueConverters->forFlowType(type_integer()),
            \is_float($value) => $this->valueConverters->forFlowType(type_float()),
            $value instanceof \DateTimeInterface => $this->valueConverters->forFlowType(type_datetime()),
            $value instanceof Uuid => $this->valueConverters->forFlowType(type_uuid()),
            $value instanceof Json => $this->valueConverters->forFlowType(type_json()),
            \is_array($value) => $this->valueConverters->forFlowType(type_list(type_string())),
            default => $this->valueConverters->forFlowType(type_string()),
        };
    }

    private function executeTransactionCommand(SqlQuery $query, callable $exceptionFactory) : void
    {
        /** @var Connection $connection */
        $connection = $this->connection;

        $result = @\pg_query($connection, $query->toSql());

        if ($result === false) {
            $this->transactionContext->reset();

            throw $exceptionFactory(\pg_last_error($connection) ?: 'Unknown error');
        }

        \pg_free_result($result);
    }

    /**
     * @param array<int, mixed> $parameters
     */
    private function query(SqlQuery|string $sql, array $parameters) : Result
    {
        $this->assertConnected();

        /** @var Connection $connection */
        $connection = $this->connection;

        $query = $sql instanceof SqlQuery ? $sql->toSql() : $sql;
        $params = $this->convertParameters($parameters);
        $result = @\pg_query_params($connection, $query, $params);

        if ($result === false) {
            throw QueryException::executionFailed($query, \pg_last_error($connection) ?: 'Unknown error');
        }

        return $result;
    }

    private function resolveMapper(?RowMapper $mapper) : RowMapper
    {
        $resolved = $mapper ?? $this->defaultMapper;

        if ($resolved === null) {
            throw MappingException::noMapperConfigured();
        }

        return $resolved;
    }

    private static function assertExtensionLoaded() : void
    {
        if (!\extension_loaded('pgsql')) {
            throw ConnectionException::extensionNotLoaded('pgsql');
        }
    }
}
