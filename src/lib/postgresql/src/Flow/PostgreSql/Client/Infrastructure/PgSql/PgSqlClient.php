<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Infrastructure\PgSql;

use function Flow\PostgreSql\DSL\{begin, commit, listen, release_savepoint, rollback, savepoint, unlisten};
use Flow\PostgreSql\AST\Transformers\{ExplainConfig, ExplainModifier};
use Flow\PostgreSql\Client\{Client, ConnectionParameters, Cursor, Notification, RowMapper, TransactionContext, TypedValue};
use Flow\PostgreSql\Client\Exception\{ConnectionException, PostgreSqlError, QueryException, ResultException, TransactionException, ValueConversionException};
use Flow\PostgreSql\Client\Types\{ResultCaster, ValueConverters, ValueType};
use Flow\PostgreSql\Explain\ExplainParser;
use Flow\PostgreSql\Explain\Plan\Plan;
use Flow\PostgreSql\Parser;
use Flow\PostgreSql\QueryBuilder\Sql;
use PgSql\{Connection, Result};

final class PgSqlClient implements Client
{
    private bool $autoCommit = true;

    /** @var array<string, true> */
    private array $listeningChannels = [];

    private readonly ResultCaster $resultCaster;

    private readonly TransactionContext $transactionContext;

    private function __construct(
        private ?Connection $connection,
        private readonly ConnectionParameters $connectionParameters,
        private readonly ValueConverters $valueConverters,
    ) {
        $this->resultCaster = new ResultCaster();
        $this->transactionContext = new TransactionContext();
    }

    /**
     * Connect using ConnectionParameters.
     */
    public static function connect(
        ConnectionParameters $params,
        ?ValueConverters $valueConverters = null,
    ) : self {
        if (!\extension_loaded('pgsql')) {
            throw ConnectionException::extensionNotLoaded('pgsql');
        }

        \error_clear_last();
        $connection = @\pg_connect($params->toString(), \PGSQL_CONNECT_FORCE_NEW);

        if ($connection === false) {
            $error = \error_get_last();

            throw ConnectionException::connectionFailed($error['message'] ?? 'Unknown error');
        }

        return new self(
            $connection,
            $params,
            $valueConverters ?? ValueConverters::create(),
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
                static fn (string $error) => TransactionException::savepointFailed($savepointName, $error)
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
                static fn (string $error) => TransactionException::releaseSavepointFailed($savepointName, $error)
            );
        }
    }

    public function converters() : ValueConverters
    {
        return $this->valueConverters;
    }

    public function cursor(Sql|string $sql, array $parameters = []) : Cursor
    {
        $result = $this->query($sql, $parameters);

        return new PgSqlCursor($result);
    }

    public function execute(Sql|string $sql, array $parameters = []) : int
    {
        $result = $this->query($sql, $parameters);
        $affected = \pg_affected_rows($result);
        \pg_free_result($result);

        return $affected;
    }

    public function explain(Sql|string $sql, array $parameters = [], ?ExplainConfig $config = null) : Plan
    {
        $config ??= ExplainConfig::forAnalysis();
        $query = $sql instanceof Sql ? $sql->toSql() : $sql;

        $parsed = (new Parser())->parse($query);
        $parsed->traverse(new ExplainModifier($config));
        $explainQuery = $parsed->deparse();

        $jsonOutput = $this->fetchScalarString($explainQuery, $parameters);

        return (new ExplainParser())->parse($jsonOutput);
    }

    public function fetch(Sql|string $sql, array $parameters = []) : ?array
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

    public function fetchAll(Sql|string $sql, array $parameters = []) : array
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
        RowMapper $mapper,
        Sql|string $sql,
        array $parameters = [],
    ) : array {
        return \array_values(\array_map(
            static fn (array $row) => $mapper->map($row),
            $this->fetchAll($sql, $parameters),
        ));
    }

    public function fetchInto(
        RowMapper $mapper,
        Sql|string $sql,
        array $parameters = [],
    ) : mixed {
        $row = $this->fetch($sql, $parameters);

        if ($row === null) {
            return null;
        }

        return $mapper->map($row);
    }

    public function fetchOne(Sql|string $sql, array $parameters = []) : array
    {
        $result = $this->query($sql, $parameters);
        $count = \pg_num_rows($result);

        if ($count === 0) {
            \pg_free_result($result);

            throw ResultException::noRowsFound();
        }

        if ($count > 1) {
            \pg_free_result($result);

            throw ResultException::tooManyRows($count);
        }

        $row = \pg_fetch_assoc($result);

        if ($row === false) {
            \pg_free_result($result);

            throw ResultException::noRowsFound();
        }

        $converted = $this->convertRow($result, $row);
        \pg_free_result($result);

        return $converted;
    }

    public function fetchOneInto(
        RowMapper $mapper,
        Sql|string $sql,
        array $parameters = [],
    ) : mixed {
        return $mapper->map($this->fetchOne($sql, $parameters));
    }

    public function fetchScalar(Sql|string $sql, array $parameters = []) : mixed
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
            $value = $this->resultCaster->cast($value, \pg_field_type($result, 0));
        }

        \pg_free_result($result);

        return $value;
    }

    public function fetchScalarBool(Sql|string $sql, array $parameters = []) : bool
    {
        $value = $this->fetchScalar($sql, $parameters);

        if (!\is_bool($value)) {
            throw ResultException::unexpectedScalarType('bool', \get_debug_type($value));
        }

        return $value;
    }

    public function fetchScalarFloat(Sql|string $sql, array $parameters = []) : float
    {
        $value = $this->fetchScalar($sql, $parameters);

        if (!\is_float($value)) {
            throw ResultException::unexpectedScalarType('float', \get_debug_type($value));
        }

        return $value;
    }

    public function fetchScalarInt(Sql|string $sql, array $parameters = []) : int
    {
        $value = $this->fetchScalar($sql, $parameters);

        if (!\is_int($value)) {
            throw ResultException::unexpectedScalarType('int', \get_debug_type($value));
        }

        return $value;
    }

    public function fetchScalarString(Sql|string $sql, array $parameters = []) : string
    {
        $value = $this->fetchScalar($sql, $parameters);

        if (!\is_string($value)) {
            throw ResultException::unexpectedScalarType('string', \get_debug_type($value));
        }

        return $value;
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
                throw ResultException::sequenceNotUsed($sequenceName);
            }

            throw $e;
        }

        if ($result === null) {
            throw ResultException::sequenceNotUsed($sequenceName);
        }

        /** @var int|string $result */
        return $result;
    }

    public function listen(string $channel) : void
    {
        if (\array_key_exists($channel, $this->listeningChannels)) {
            return;
        }

        $this->execute(listen($channel));
        $this->listeningChannels[$channel] = true;
    }

    public function parameters() : ConnectionParameters
    {
        return $this->connectionParameters;
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

    public function unlisten(string $channel) : void
    {
        if (!\array_key_exists($channel, $this->listeningChannels)) {
            return;
        }

        $this->execute(unlisten($channel));
        unset($this->listeningChannels[$channel]);
    }

    public function wait(int $milliseconds) : ?Notification
    {
        if ($milliseconds < 0) {
            throw new \InvalidArgumentException(
                \sprintf('Timeout must be non-negative, got %d', $milliseconds),
            );
        }

        $this->assertConnected();

        /** @var Connection $connection */
        $connection = $this->connection;

        $immediate = @\pg_get_notify($connection, \PGSQL_ASSOC);

        if (\is_array($immediate)) {
            return self::notificationFromRaw($immediate);
        }

        if ($milliseconds === 0) {
            return null;
        }

        $socket = @\pg_socket($connection);

        if ($socket === false) {
            throw ConnectionException::notificationWaitFailed('pg_socket() failed to return connection socket');
        }

        $deadlineNs = \hrtime(true) + $milliseconds * 1_000_000;

        while (true) {
            $remainingNs = $deadlineNs - \hrtime(true);

            if ($remainingNs <= 0) {
                return null;
            }

            $read = [$socket];
            $write = null;
            $except = null;
            $selected = @\stream_select(
                $read,
                $write,
                $except,
                (int) \intdiv($remainingNs, 1_000_000_000),
                (int) \intdiv($remainingNs % 1_000_000_000, 1000),
            );

            if ($selected === false) {
                $lastError = \error_get_last();

                throw ConnectionException::notificationWaitFailed(
                    'stream_select() failed: ' . ($lastError['message'] ?? 'unknown error'),
                );
            }

            if ($selected === 0) {
                return null;
            }

            if (!@\pg_consume_input($connection)) {
                throw ConnectionException::notificationWaitFailed(
                    'pg_consume_input() failed: ' . \pg_last_error($connection),
                );
            }

            $raw = @\pg_get_notify($connection, \PGSQL_ASSOC);

            if (\is_array($raw)) {
                return self::notificationFromRaw($raw);
            }
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
                $converter = $this->valueConverters->forValueType($value->targetType);
                $converted[] = $converter->toDatabase($value->value);
            } else {
                if (\is_array($value)) {
                    throw ValueConversionException::ambiguousArrayType();
                }

                $converted[] = $this->valueConverters->forValueType(ValueType::TEXT)->toDatabase($value);
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
            $key = (string) $column;

            if ($value === null) {
                $converted[$key] = null;
            } else {
                $converted[$key] = $this->resultCaster->cast($value, \pg_field_type($result, $i));
            }

            $i++;
        }

        return $converted;
    }

    private function executeTransactionCommand(Sql $query, callable $exceptionFactory) : void
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

    private function extractError(Connection $connection, ?Result $result) : PostgreSqlError
    {
        if ($result !== null) {
            $sqlState = \pg_result_error_field($result, \PGSQL_DIAG_SQLSTATE);
            $message = \pg_result_error_field($result, \PGSQL_DIAG_MESSAGE_PRIMARY);
            $detail = \pg_result_error_field($result, \PGSQL_DIAG_MESSAGE_DETAIL);
            $hint = \pg_result_error_field($result, \PGSQL_DIAG_MESSAGE_HINT);
            $schema = \pg_result_error_field($result, \PGSQL_DIAG_SCHEMA_NAME);
            $table = \pg_result_error_field($result, \PGSQL_DIAG_TABLE_NAME);
            $column = \pg_result_error_field($result, \PGSQL_DIAG_COLUMN_NAME);
            $constraint = \pg_result_error_field($result, \PGSQL_DIAG_CONSTRAINT_NAME);
            $position = \pg_result_error_field($result, \PGSQL_DIAG_STATEMENT_POSITION);

            if ($sqlState !== false && $sqlState !== null) {
                return PostgreSqlError::fromDiagnostics(
                    $sqlState,
                    ($message !== false && $message !== null) ? $message : (\pg_result_error($result) ?: 'Unknown error'),
                    ($detail !== false && $detail !== null) ? $detail : null,
                    ($hint !== false && $hint !== null) ? $hint : null,
                    ($schema !== false && $schema !== null) ? $schema : null,
                    ($table !== false && $table !== null) ? $table : null,
                    ($column !== false && $column !== null) ? $column : null,
                    ($constraint !== false && $constraint !== null) ? $constraint : null,
                    ($position !== false && $position !== null) ? (int) $position : null,
                );
            }
        }

        $errorMessage = \pg_last_error($connection);

        return PostgreSqlError::unknown($errorMessage !== '' ? $errorMessage : 'Unknown error');
    }

    /**
     * @param array<int, mixed> $parameters
     */
    private function query(Sql|string $sql, array $parameters) : Result
    {
        $this->assertConnected();

        /** @var Connection $connection */
        $connection = $this->connection;

        $query = $sql instanceof Sql ? $sql->toSql() : $sql;
        $convertedParams = $this->convertParameters($parameters);

        $success = @\pg_send_query_params($connection, $query, $convertedParams);

        if ($success === false) {
            throw QueryException::executionFailed(
                $query,
                $this->extractError($connection, null)
            );
        }

        $result = \pg_get_result($connection);

        if ($result === false) {
            throw QueryException::executionFailed(
                $query,
                $this->extractError($connection, null)
            );
        }

        $status = \pg_result_status($result);

        if ($status === \PGSQL_FATAL_ERROR || $status === \PGSQL_NONFATAL_ERROR) {
            $error = $this->extractError($connection, $result);
            \pg_free_result($result);

            throw QueryException::executionFailed($query, $error);
        }

        return $result;
    }

    /**
     * @param array<array-key, mixed> $raw
     */
    private static function notificationFromRaw(array $raw) : Notification
    {
        $channel = $raw['message'] ?? '';
        $payload = $raw['payload'] ?? '';
        $pid = $raw['pid'] ?? 0;

        if (!\is_string($channel) || !\is_string($payload) || !\is_int($pid)) {
            throw ConnectionException::notificationWaitFailed(
                'Malformed notification payload from pg_get_notify()',
            );
        }

        return new Notification($channel, $payload, $pid);
    }
}
