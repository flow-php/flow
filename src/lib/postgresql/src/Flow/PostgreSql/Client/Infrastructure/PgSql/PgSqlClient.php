<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Infrastructure\PgSql;

use Flow\PostgreSql\AST\Transformers\ExplainConfig;
use Flow\PostgreSql\AST\Transformers\ExplainModifier;
use Flow\PostgreSql\Client\Client;
use Flow\PostgreSql\Client\ConnectionParameters;
use Flow\PostgreSql\Client\Context as ClientContext;
use Flow\PostgreSql\Client\ConvertedParameters;
use Flow\PostgreSql\Client\Cursor;
use Flow\PostgreSql\Client\DescribeQuery;
use Flow\PostgreSql\Client\Exception\ConnectionException;
use Flow\PostgreSql\Client\Exception\NoResultException;
use Flow\PostgreSql\Client\Exception\PostgreSqlError;
use Flow\PostgreSql\Client\Exception\QueryException;
use Flow\PostgreSql\Client\Exception\ResultException;
use Flow\PostgreSql\Client\Exception\TooManyRowsException;
use Flow\PostgreSql\Client\Exception\TransactionException;
use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Notification;
use Flow\PostgreSql\Client\Query;
use Flow\PostgreSql\Client\RowMapper;
use Flow\PostgreSql\Client\RowMapper\Context;
use Flow\PostgreSql\Client\TransactionContext;
use Flow\PostgreSql\Client\TypedValue;
use Flow\PostgreSql\Client\Types\ResultCaster;
use Flow\PostgreSql\Client\Types\ValueConverters;
use Flow\PostgreSql\Client\Types\ValueType;
use Flow\PostgreSql\Explain\ExplainParser;
use Flow\PostgreSql\Explain\Plan\Plan;
use Flow\PostgreSql\Parser;
use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;
use Flow\PostgreSql\QueryBuilder\Sql;
use InvalidArgumentException;
use PgSql\Connection;
use PgSql\Result;
use Throwable;

use function array_fill;
use function array_filter;
use function array_key_exists;
use function array_map;
use function array_values;
use function count;
use function error_clear_last;
use function error_get_last;
use function extension_loaded;
use function Flow\PostgreSql\DSL\begin;
use function Flow\PostgreSql\DSL\column_type_from_string;
use function Flow\PostgreSql\DSL\commit;
use function Flow\PostgreSql\DSL\listen;
use function Flow\PostgreSql\DSL\release_savepoint;
use function Flow\PostgreSql\DSL\rollback;
use function Flow\PostgreSql\DSL\savepoint;
use function Flow\PostgreSql\DSL\unlisten;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_union;
use function hrtime;
use function intdiv;
use function is_array;
use function is_int;
use function is_string;
use function pg_affected_rows;
use function pg_close;
use function pg_connect;
use function pg_connection_status;
use function pg_consume_input;
use function pg_fetch_all;
use function pg_fetch_assoc;
use function pg_fetch_result;
use function pg_field_type;
use function pg_free_result;
use function pg_get_notify;
use function pg_get_result;
use function pg_last_error;
use function pg_num_rows;
use function pg_query;
use function pg_result_error;
use function pg_result_error_field;
use function pg_result_status;
use function pg_send_query_params;
use function pg_socket;
use function sprintf;
use function str_contains;
use function stream_select;

use const PGSQL_ASSOC;
use const PGSQL_CONNECT_FORCE_NEW;
use const PGSQL_CONNECTION_OK;
use const PGSQL_DIAG_COLUMN_NAME;
use const PGSQL_DIAG_CONSTRAINT_NAME;
use const PGSQL_DIAG_MESSAGE_DETAIL;
use const PGSQL_DIAG_MESSAGE_HINT;
use const PGSQL_DIAG_MESSAGE_PRIMARY;
use const PGSQL_DIAG_SCHEMA_NAME;
use const PGSQL_DIAG_SQLSTATE;
use const PGSQL_DIAG_STATEMENT_POSITION;
use const PGSQL_DIAG_TABLE_NAME;
use const PGSQL_FATAL_ERROR;
use const PGSQL_NONFATAL_ERROR;

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
        private readonly ClientContext $clientContext = new ClientContext(),
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
        ?ClientContext $context = null,
    ): self {
        if (!extension_loaded('pgsql')) {
            throw ConnectionException::extensionNotLoaded('pgsql');
        }

        error_clear_last();
        $connection = @pg_connect($params->toString(), PGSQL_CONNECT_FORCE_NEW);

        if ($connection === false) {
            $error = error_get_last();

            throw ConnectionException::connectionFailed($error['message'] ?? 'Unknown error');
        }

        return new self(
            $connection,
            $params,
            $valueConverters ?? ValueConverters::create(),
            $context ?? new ClientContext(),
        );
    }

    public function beginTransaction(): void
    {
        $this->assertConnected();

        $savepointName = $this->transactionContext->begin();

        if ($savepointName === null) {
            $this->executeTransactionCommand(begin(), TransactionException::beginFailed(...));
        } else {
            $this->executeTransactionCommand(
                savepoint($savepointName),
                static fn(string $error) => TransactionException::savepointFailed($savepointName, $error),
            );
        }
    }

    public function close(): void
    {
        if ($this->connection !== null) {
            @pg_close($this->connection);
            $this->connection = null;
        }
    }

    public function commit(): void
    {
        $this->assertConnected();

        $savepointName = $this->transactionContext->commit();

        if ($savepointName === null) {
            $this->executeTransactionCommand(commit(), TransactionException::commitFailed(...));
        } else {
            $this->executeTransactionCommand(
                release_savepoint($savepointName),
                static fn(string $error) => TransactionException::releaseSavepointFailed($savepointName, $error),
            );
        }
    }

    public function converters(): ValueConverters
    {
        return $this->valueConverters;
    }

    public function cursor(Sql|string $sql, array $parameters = []): Cursor
    {
        $result = $this->query($sql, $parameters);

        return new PgSqlCursor($result, $this->buildContext($sql, $parameters));
    }

    /**
     * @return list<array{name: string, type: ColumnType}>
     */
    public function describe(Sql|string $sql, array $parameters = []): array
    {
        $result = $this->query((new DescribeQuery())->of($sql), array_fill(0, count($parameters), null));

        try {
            return array_map(static fn(array $column): array => [
                'name' => $column['name'],
                'type' => column_type_from_string($column['type']),
            ], (new ResultColumns())->of($result));
        } finally {
            pg_free_result($result);
        }
    }

    public function execute(Sql|string $sql, array|ConvertedParameters $parameters = []): int
    {
        if ($parameters instanceof ConvertedParameters) {
            $this->assertConnected();

            $result = $this->send($sql instanceof Sql ? $sql->toSql() : $sql, $parameters->values);
        } else {
            $result = $this->query($sql, $parameters);
        }

        $affected = pg_affected_rows($result);
        pg_free_result($result);

        return $affected;
    }

    public function explain(Sql|string $sql, array $parameters = [], ?ExplainConfig $config = null): Plan
    {
        $config ??= ExplainConfig::forAnalysis();
        $query = $sql instanceof Sql ? $sql->toSql() : $sql;

        $parsed = (new Parser())->parse($query);
        $parsed->traverse(new ExplainModifier($config));
        $explainQuery = $parsed->deparse();

        $jsonOutput = $this->fetchScalarString($explainQuery, $parameters);

        return (new ExplainParser())->parse($jsonOutput);
    }

    public function fetch(Sql|string $sql, array $parameters = []): ?array
    {
        $result = $this->query($sql, $parameters);
        $row = pg_fetch_assoc($result);

        if ($row === false) {
            pg_free_result($result);

            return null;
        }

        $converted = $this->convertRow($row, $this->convertingColumns($result));
        pg_free_result($result);

        return $converted;
    }

    public function fetchAll(Sql|string $sql, array $parameters = []): array
    {
        $result = $this->query($sql, $parameters);
        $rows = pg_fetch_all($result) ?: [];
        $columns = $this->convertingColumns($result);
        $converted = [];

        foreach ($rows as $row) {
            $converted[] = $this->convertRow($row, $columns);
        }

        $rows = $converted;

        pg_free_result($result);

        return $rows;
    }

    public function fetchAllInto(RowMapper $mapper, Sql|string $sql, array $parameters = []): array
    {
        $context = $this->buildContext($sql, $parameters);

        $mapped = [];

        foreach ($this->fetchAll($sql, $parameters) as $row) {
            $mapped[] = $mapper->map($row, $context);
        }

        return $mapped;
    }

    public function fetchInto(RowMapper $mapper, Sql|string $sql, array $parameters = []): mixed
    {
        $row = $this->fetch($sql, $parameters);

        if ($row === null) {
            return null;
        }

        return $mapper->map($row, $this->buildContext($sql, $parameters));
    }

    public function fetchOne(Sql|string $sql, array $parameters = []): ?array
    {
        $result = $this->query($sql, $parameters);
        $count = pg_num_rows($result);

        if ($count === 0) {
            pg_free_result($result);

            return null;
        }

        if ($count > 1) {
            pg_free_result($result);

            throw new TooManyRowsException($count);
        }

        $row = pg_fetch_assoc($result);

        if ($row === false) {
            pg_free_result($result);

            return null;
        }

        $converted = $this->convertRow($row, $this->convertingColumns($result));
        pg_free_result($result);

        return $converted;
    }

    public function fetchOneInto(RowMapper $mapper, Sql|string $sql, array $parameters = []): mixed
    {
        $row = $this->fetchOne($sql, $parameters);

        if ($row === null) {
            return null;
        }

        return $mapper->map($row, $this->buildContext($sql, $parameters));
    }

    public function fetchScalar(Sql|string $sql, array $parameters = []): mixed
    {
        $result = $this->query($sql, $parameters);

        if (pg_num_rows($result) === 0) {
            pg_free_result($result);

            return null;
        }

        $value = pg_fetch_result($result, 0, 0);

        if ($value === false) {
            pg_free_result($result);

            return null;
        }

        if ($value !== null) {
            $value = $this->resultCaster->cast($value, pg_field_type($result, 0));
        }

        pg_free_result($result);

        return $value;
    }

    public function fetchScalarBool(Sql|string $sql, array $parameters = []): bool
    {
        return type_boolean()->assert($this->fetchScalar($sql, $parameters));
    }

    public function fetchScalarFloat(Sql|string $sql, array $parameters = []): float
    {
        return type_float()->assert($this->fetchScalar($sql, $parameters));
    }

    public function fetchScalarInt(Sql|string $sql, array $parameters = []): int
    {
        return type_integer()->assert($this->fetchScalar($sql, $parameters));
    }

    public function fetchScalarString(Sql|string $sql, array $parameters = []): string
    {
        return type_string()->assert($this->fetchScalar($sql, $parameters));
    }

    public function fetchSingle(Sql|string $sql, array $parameters = []): array
    {
        $row = $this->fetchOne($sql, $parameters);

        if ($row === null) {
            throw new NoResultException();
        }

        return $row;
    }

    public function fetchSingleInto(RowMapper $mapper, Sql|string $sql, array $parameters = []): mixed
    {
        return $mapper->map($this->fetchSingle($sql, $parameters), $this->buildContext($sql, $parameters));
    }

    public function getTransactionNestingLevel(): int
    {
        return $this->transactionContext->getNestingLevel();
    }

    public function isAutoCommit(): bool
    {
        return $this->autoCommit;
    }

    public function isConnected(): bool
    {
        return $this->connection !== null && pg_connection_status($this->connection) === PGSQL_CONNECTION_OK;
    }

    public function lastInsertId(string $sequenceName): int|string
    {
        try {
            return type_union(type_integer(), type_string())->assert(
                $this->fetchScalar('SELECT currval($1)', [$sequenceName]) ?? throw ResultException::sequenceNotUsed(
                    $sequenceName,
                ),
            );
        } catch (QueryException $e) {
            if (str_contains($e->getMessage(), 'is not yet defined in this session')) {
                throw ResultException::sequenceNotUsed($sequenceName);
            }

            throw $e;
        }
    }

    public function listen(string $channel): void
    {
        if (array_key_exists($channel, $this->listeningChannels)) {
            return;
        }

        $this->execute(listen($channel));
        $this->listeningChannels[$channel] = true;
    }

    public function parameters(): ConnectionParameters
    {
        return $this->connectionParameters;
    }

    public function rollBack(): void
    {
        $this->assertConnected();

        $savepointName = $this->transactionContext->rollBack();

        if ($savepointName === null) {
            $this->executeTransactionCommand(rollback(), TransactionException::rollbackFailed(...));
        } else {
            $this->executeTransactionCommand(
                rollback()->toSavepoint($savepointName),
                static fn(string $error) => TransactionException::rollbackToSavepointFailed($savepointName, $error),
            );
        }
    }

    public function setAutoCommit(bool $autoCommit): void
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

    public function transaction(callable $callback): mixed
    {
        $this->beginTransaction();

        try {
            $returnValue = $callback($this);
            $this->commit();

            return $returnValue;
        } catch (Throwable $e) {
            $this->rollBack();

            throw $e;
        }
    }

    public function unlisten(string $channel): void
    {
        if (!array_key_exists($channel, $this->listeningChannels)) {
            return;
        }

        $this->execute(unlisten($channel));
        unset($this->listeningChannels[$channel]);
    }

    public function wait(int $milliseconds): ?Notification
    {
        if ($milliseconds < 0) {
            throw new InvalidArgumentException(sprintf('Timeout must be non-negative, got %d', $milliseconds));
        }

        $this->assertConnected();

        /** @var Connection $connection */
        $connection = $this->connection;

        $immediate = self::tryGetNotify($connection);

        if ($immediate !== null) {
            return self::notificationFromRaw($immediate);
        }

        if ($milliseconds === 0) {
            return null;
        }

        $socket = @pg_socket($connection);

        if ($socket === false) {
            throw ConnectionException::notificationWaitFailed('pg_socket() failed to return connection socket');
        }

        $deadlineNs = hrtime(true) + ($milliseconds * 1_000_000);

        while (true) {
            $remainingNs = $deadlineNs - hrtime(true);

            if ($remainingNs <= 0) {
                return null;
            }

            $read = [$socket];
            $write = null;
            $except = null;
            $selected = @stream_select(
                $read,
                $write,
                $except,
                (int) intdiv((int) $remainingNs, 1_000_000_000),
                (int) intdiv((int) $remainingNs % 1_000_000_000, 1000),
            );

            if ($selected === false) {
                $lastError = error_get_last();

                throw ConnectionException::notificationWaitFailed(
                    'stream_select() failed: ' . ($lastError['message'] ?? 'unknown error'),
                );
            }

            if ($selected === 0) {
                return null;
            }

            if (!@pg_consume_input($connection)) {
                throw ConnectionException::notificationWaitFailed(
                    'pg_consume_input() failed: ' . pg_last_error($connection),
                );
            }

            $raw = self::tryGetNotify($connection);

            if ($raw !== null) {
                return self::notificationFromRaw($raw);
            }
        }
    }

    private function assertConnected(): void
    {
        if (!$this->isConnected()) {
            throw ConnectionException::notConnected();
        }
    }

    /**
     * @param list<mixed> $parameters
     */
    private function buildContext(Sql|string $sql, array $parameters): Context
    {
        return new Context(query: new Query($sql, $parameters), client: $this, clientContext: $this->clientContext);
    }

    /**
     * @param list<mixed> $parameters
     *
     * @return array<int, null|string>
     */
    private function convertParameters(array $parameters): array
    {
        return array_values(array_map($this->convertParameter(...), $parameters));
    }

    private function convertParameter(mixed $value): ?string
    {
        if ($value === null) {
            return null;
        }

        if ($value instanceof TypedValue) {
            return $this->valueConverters->forValueType($value->targetType)->toDatabase($value->value);
        }

        if (is_array($value)) {
            throw ValueConversionException::ambiguousArrayType();
        }

        return $this->valueConverters->forValueType(ValueType::TEXT)->toDatabase($value);
    }

    /**
     * @param array<array-key, mixed> $row
     * @param array<string, string> $columns the result's converting columns, from convertingColumns()
     *
     * @return array<string, mixed>
     */
    private function convertRow(array $row, array $columns): array
    {
        foreach ($columns as $column => $type) {
            // @mago-ignore analysis:mixed-assignment
            $value = $row[$column] ?? null;

            if (is_string($value)) {
                $row[$column] = $this->resultCaster->cast($value, $type);
            }
        }

        /** @var array<string, mixed> $row */
        return $row;
    }

    /**
     * The columns whose values ResultCaster converts, by name. By name, last wins, exactly as pg_fetch_all()
     * collapses duplicate output names. A positional lookup applies the wrong column's type to the surviving
     * value: for SELECT id AS a, label AS a it casts the text through int8 and yields 0.
     *
     * @return array<string, string>
     */
    private function convertingColumns(Result $result): array
    {
        $types = [];

        foreach ((new ResultColumns())->of($result) as $column) {
            $types[$column['name']] = $column['type'];
        }

        return array_filter($types, $this->resultCaster->converts(...));
    }

    /**
     * @param callable(string): \Throwable $exceptionFactory
     */
    private function executeTransactionCommand(Sql $query, callable $exceptionFactory): void
    {
        /** @var Connection $connection */
        $connection = $this->connection;

        $result = @pg_query($connection, $query->toSql());

        if ($result === false) {
            $this->transactionContext->reset();

            throw $exceptionFactory(pg_last_error($connection) ?: 'Unknown error');
        }

        pg_free_result($result);
    }

    private function extractError(Connection $connection, ?Result $result): PostgreSqlError
    {
        if ($result !== null) {
            $sqlState = pg_result_error_field($result, PGSQL_DIAG_SQLSTATE);
            $message = pg_result_error_field($result, PGSQL_DIAG_MESSAGE_PRIMARY);
            $detail = pg_result_error_field($result, PGSQL_DIAG_MESSAGE_DETAIL);
            $hint = pg_result_error_field($result, PGSQL_DIAG_MESSAGE_HINT);
            $schema = pg_result_error_field($result, PGSQL_DIAG_SCHEMA_NAME);
            $table = pg_result_error_field($result, PGSQL_DIAG_TABLE_NAME);
            $column = pg_result_error_field($result, PGSQL_DIAG_COLUMN_NAME);
            $constraint = pg_result_error_field($result, PGSQL_DIAG_CONSTRAINT_NAME);
            $position = pg_result_error_field($result, PGSQL_DIAG_STATEMENT_POSITION);

            if ($sqlState !== false && $sqlState !== null) {
                return PostgreSqlError::fromDiagnostics(
                    $sqlState,
                    $message !== false && $message !== null ? $message : (pg_result_error($result) ?: 'Unknown error'),
                    $detail !== false && $detail !== null ? $detail : null,
                    $hint !== false && $hint !== null ? $hint : null,
                    $schema !== false && $schema !== null ? $schema : null,
                    $table !== false && $table !== null ? $table : null,
                    $column !== false && $column !== null ? $column : null,
                    $constraint !== false && $constraint !== null ? $constraint : null,
                    $position !== false && $position !== null ? (int) $position : null,
                );
            }
        }

        $errorMessage = pg_last_error($connection);

        return PostgreSqlError::unknown($errorMessage !== '' ? $errorMessage : 'Unknown error');
    }

    /**
     * @param list<mixed> $parameters
     */
    private function query(Sql|string $sql, array $parameters): Result
    {
        $this->assertConnected();

        return $this->send($sql instanceof Sql ? $sql->toSql() : $sql, $this->convertParameters($parameters));
    }

    /**
     * @param array<int, null|string> $convertedParams
     */
    private function send(string $query, array $convertedParams): Result
    {
        /** @var Connection $connection */
        $connection = $this->connection;

        $success = @pg_send_query_params($connection, $query, $convertedParams);

        if ($success === false) {
            throw QueryException::executionFailed($query, $this->extractError($connection, null));
        }

        $result = pg_get_result($connection);

        if ($result === false) {
            throw QueryException::executionFailed($query, $this->extractError($connection, null));
        }

        while (pg_get_result($connection) !== false) {
        }

        $status = pg_result_status($result);

        if ($status === PGSQL_FATAL_ERROR || $status === PGSQL_NONFATAL_ERROR) {
            $error = $this->extractError($connection, $result);
            pg_free_result($result);

            throw QueryException::executionFailed($query, $error);
        }

        return $result;
    }

    /**
     * @return null|array<array-key, mixed>
     */
    private static function tryGetNotify(Connection $connection): ?array
    {
        $result = type_union(type_array(), type_boolean())->assert(@pg_get_notify($connection, PGSQL_ASSOC));

        return is_array($result) ? $result : null;
    }

    /**
     * @param array<array-key, mixed> $raw
     */
    private static function notificationFromRaw(array $raw): Notification
    {
        if (
            !is_string($raw['message'] ?? null)
            || !is_string($raw['payload'] ?? null)
            || !is_int($raw['pid'] ?? null)
        ) {
            throw ConnectionException::notificationWaitFailed('Malformed notification payload from pg_get_notify()');
        }

        return new Notification($raw['message'], $raw['payload'], $raw['pid']);
    }
}
