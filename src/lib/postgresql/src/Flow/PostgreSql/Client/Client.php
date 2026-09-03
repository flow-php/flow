<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client;

use Flow\PostgreSql\AST\Transformers\ExplainConfig;
use Flow\PostgreSql\Client\Exception\ConnectionException;
use Flow\PostgreSql\Client\Exception\NoResultException;
use Flow\PostgreSql\Client\Exception\QueryException;
use Flow\PostgreSql\Client\Exception\TooManyRowsException;
use Flow\PostgreSql\Client\Exception\TransactionException;
use Flow\PostgreSql\Client\Types\ValueConverters;
use Flow\PostgreSql\Explain\Plan\Plan;
use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;
use Flow\PostgreSql\QueryBuilder\Sql;

interface Client
{
    /**
     * Begin a transaction.
     * Supports nesting via SAVEPOINTs.
     *
     * @throws TransactionException
     */
    public function beginTransaction(): void;

    /**
     * Close the connection.
     */
    public function close(): void;

    /**
     * Commit the current transaction.
     * If nested, releases the savepoint.
     *
     * @throws TransactionException
     */
    public function commit(): void;

    /**
     * Get the value converters registry.
     */
    public function converters(): ValueConverters;

    /**
     * Get a cursor for lazy iteration over large result sets.
     * Memory efficient - rows are fetched one at a time.
     * Use cursor->map() to map rows to objects.
     *
     * @param Sql|string $sql SQL query or query builder with $1, $2, ... placeholders
     * @param list<mixed> $parameters Values bound by position to $1, $2, ... placeholders; wrap with {@see \Flow\PostgreSql\DSL\typed()} to force a specific PostgreSQL type
     *
     * @throws QueryException
     */
    public function cursor(Sql|string $sql, array $parameters = []): Cursor;

    /**
     * Describe the columns a query would produce, by executing a zero-row probe of it.
     * Column order is the query's select order. Duplicate output names are returned as many
     * times as the query projects them - see pg_fetch_assoc(), which collapses them last-wins.
     *
     * @param Sql|string $sql SQL query or query builder with $1, $2, ... placeholders
     * @param list<mixed> $parameters Only the count is used
     *
     * @throws QueryException
     *
     * @return list<array{name: string, type: ColumnType}>
     */
    public function describe(Sql|string $sql, array $parameters = []): array;

    /**
     * Execute a statement that modifies data (INSERT, UPDATE, DELETE).
     * Returns the number of affected rows.
     *
     * @param Sql|string $sql SQL statement or query builder with $1, $2, ... placeholders
     * @param list<mixed> $parameters Values bound by position to $1, $2, ... placeholders; wrap with {@see \Flow\PostgreSql\DSL\typed()} to force a specific PostgreSQL type
     *
     * @throws QueryException
     */
    public function execute(Sql|string $sql, array $parameters = []): int;

    /**
     * Execute EXPLAIN ANALYZE on a query and return the execution plan.
     * Useful for analyzing query performance.
     *
     * @param Sql|string $sql SQL query to explain
     * @param list<mixed> $parameters Values bound by position to $1, $2, ... placeholders; wrap with {@see \Flow\PostgreSql\DSL\typed()} to force a specific PostgreSQL type
     * @param null|ExplainConfig $config EXPLAIN configuration (defaults to forAnalysis())
     *
     * @throws QueryException
     */
    public function explain(Sql|string $sql, array $parameters = [], ?ExplainConfig $config = null): Plan;

    /**
     * Fetch the first row from query result.
     * Returns null if no rows found.
     *
     * @param Sql|string $sql SQL query or query builder with $1, $2, ... placeholders
     * @param list<mixed> $parameters Values bound by position to $1, $2, ... placeholders; wrap with {@see \Flow\PostgreSql\DSL\typed()} to force a specific PostgreSQL type
     *
     * @throws QueryException
     *
     * @return null|array<string, mixed>
     */
    public function fetch(Sql|string $sql, array $parameters = []): ?array;

    /**
     * Fetch all rows from query result.
     *
     * @param Sql|string $sql SQL query or query builder with $1, $2, ... placeholders
     * @param list<mixed> $parameters Values bound by position to $1, $2, ... placeholders; wrap with {@see \Flow\PostgreSql\DSL\typed()} to force a specific PostgreSQL type
     *
     * @throws QueryException
     *
     * @return array<int, array<string, mixed>>
     */
    public function fetchAll(Sql|string $sql, array $parameters = []): array;

    /**
     * Fetch all rows and map using the provided mapper.
     *
     * @template T
     *
     * @param RowMapper<T> $mapper Mapper to apply to each row
     * @param Sql|string $sql SQL query or query builder with $1, $2, ... placeholders
     * @param list<mixed> $parameters Values bound by position to $1, $2, ... placeholders; wrap with {@see \Flow\PostgreSql\DSL\typed()} to force a specific PostgreSQL type
     *
     * @throws QueryException
     *
     * @return list<T>
     */
    public function fetchAllInto(RowMapper $mapper, Sql|string $sql, array $parameters = []): array;

    /**
     * Fetch the first row and map using the provided mapper.
     * Returns null if no rows found.
     *
     * @template T
     *
     * @param RowMapper<T> $mapper Mapper to apply to the row
     * @param Sql|string $sql SQL query or query builder with $1, $2, ... placeholders
     * @param list<mixed> $parameters Values bound by position to $1, $2, ... placeholders; wrap with {@see \Flow\PostgreSql\DSL\typed()} to force a specific PostgreSQL type
     *
     * @throws QueryException
     *
     * @return null|T
     */
    public function fetchInto(RowMapper $mapper, Sql|string $sql, array $parameters = []): mixed;

    /**
     * Fetch at most one row. Returns null when the result is empty, throws when it has more than one row.
     * Use when you expect zero or one result (e.g., optional lookup by unique column).
     *
     * @param Sql|string $sql SQL query or query builder with $1, $2, ... placeholders
     * @param list<mixed> $parameters Values bound by position to $1, $2, ... placeholders; wrap with {@see \Flow\PostgreSql\DSL\typed()} to force a specific PostgreSQL type
     *
     * @throws QueryException
     * @throws TooManyRowsException When the result contains more than one row
     *
     * @return null|array<string, mixed>
     */
    public function fetchOne(Sql|string $sql, array $parameters = []): ?array;

    /**
     * Fetch at most one row and map using the provided mapper.
     * Returns null when the result is empty, throws when it has more than one row.
     *
     * @template T
     *
     * @param RowMapper<T> $mapper Mapper to apply to the row
     * @param Sql|string $sql SQL query or query builder with $1, $2, ... placeholders
     * @param list<mixed> $parameters Values bound by position to $1, $2, ... placeholders; wrap with {@see \Flow\PostgreSql\DSL\typed()} to force a specific PostgreSQL type
     *
     * @throws QueryException
     * @throws TooManyRowsException When the result contains more than one row
     *
     * @return null|T
     */
    public function fetchOneInto(RowMapper $mapper, Sql|string $sql, array $parameters = []): mixed;

    /**
     * Fetch a single scalar value from the first column of first row.
     * Ideal for COUNT(*), MAX(), MIN(), etc.
     *
     * @param Sql|string $sql SQL query or query builder with $1, $2, ... placeholders
     * @param list<mixed> $parameters Values bound by position to $1, $2, ... placeholders; wrap with {@see \Flow\PostgreSql\DSL\typed()} to force a specific PostgreSQL type
     *
     * @throws QueryException
     */
    public function fetchScalar(Sql|string $sql, array $parameters = []): mixed;

    /**
     * Fetch a single boolean value from the first column of first row.
     *
     * @param Sql|string $sql SQL query or query builder with $1, $2, ... placeholders
     * @param list<mixed> $parameters Values bound by position to $1, $2, ... placeholders; wrap with {@see \Flow\PostgreSql\DSL\typed()} to force a specific PostgreSQL type
     *
     * @throws QueryException
     */
    public function fetchScalarBool(Sql|string $sql, array $parameters = []): bool;

    /**
     * Fetch a single float value from the first column of first row.
     *
     * @param Sql|string $sql SQL query or query builder with $1, $2, ... placeholders
     * @param list<mixed> $parameters Values bound by position to $1, $2, ... placeholders; wrap with {@see \Flow\PostgreSql\DSL\typed()} to force a specific PostgreSQL type
     *
     * @throws QueryException
     */
    public function fetchScalarFloat(Sql|string $sql, array $parameters = []): float;

    /**
     * Fetch a single integer value from the first column of first row.
     * Ideal for COUNT(*), MAX(), MIN(), etc.
     *
     * @param Sql|string $sql SQL query or query builder with $1, $2, ... placeholders
     * @param list<mixed> $parameters Values bound by position to $1, $2, ... placeholders; wrap with {@see \Flow\PostgreSql\DSL\typed()} to force a specific PostgreSQL type
     *
     * @throws QueryException
     */
    public function fetchScalarInt(Sql|string $sql, array $parameters = []): int;

    /**
     * Fetch a single string value from the first column of first row.
     *
     * @param Sql|string $sql SQL query or query builder with $1, $2, ... placeholders
     * @param list<mixed> $parameters Values bound by position to $1, $2, ... placeholders; wrap with {@see \Flow\PostgreSql\DSL\typed()} to force a specific PostgreSQL type
     *
     * @throws QueryException
     */
    public function fetchScalarString(Sql|string $sql, array $parameters = []): string;

    /**
     * Fetch exactly one row. Throws if result has 0 or more than 1 row.
     * Use when you expect precisely one result (e.g., SELECT by primary key, INSERT ... RETURNING).
     *
     * @param Sql|string $sql SQL query or query builder with $1, $2, ... placeholders
     * @param list<mixed> $parameters Values bound by position to $1, $2, ... placeholders; wrap with {@see \Flow\PostgreSql\DSL\typed()} to force a specific PostgreSQL type
     *
     * @throws QueryException
     * @throws NoResultException When the result is empty
     * @throws TooManyRowsException When the result contains more than one row
     *
     * @return array<string, mixed>
     */
    public function fetchSingle(Sql|string $sql, array $parameters = []): array;

    /**
     * Fetch exactly one row and map using the provided mapper.
     * Throws if result has 0 or more than 1 row.
     *
     * @template T
     *
     * @param RowMapper<T> $mapper Mapper to apply to the row
     * @param Sql|string $sql SQL query or query builder with $1, $2, ... placeholders
     * @param list<mixed> $parameters Values bound by position to $1, $2, ... placeholders; wrap with {@see \Flow\PostgreSql\DSL\typed()} to force a specific PostgreSQL type
     *
     * @throws QueryException
     * @throws NoResultException When the result is empty
     * @throws TooManyRowsException When the result contains more than one row
     *
     * @return T
     */
    public function fetchSingleInto(RowMapper $mapper, Sql|string $sql, array $parameters = []): mixed;

    /**
     * Get the current transaction nesting level.
     * 0 = no active transaction, 1 = top-level, 2+ = nested.
     */
    public function getTransactionNestingLevel(): int;

    /**
     * Check if auto-commit mode is enabled.
     * When enabled (default), each query is executed in its own transaction.
     * When disabled, a transaction is implicitly started and remains open until
     * commit() or rollBack() is called.
     */
    public function isAutoCommit(): bool;

    /**
     * Check if the connection is alive.
     */
    public function isConnected(): bool;

    /**
     * Get the last inserted ID from a sequence.
     * PostgreSQL requires a sequence name - there's no concept of "last insert ID" without it.
     *
     * Preferred alternative: Use RETURNING clause in INSERT statements:
     * $row = $client->fetch('INSERT INTO users (name) VALUES ($1) RETURNING id', ['John']);
     *
     * @param string $sequenceName The name of the sequence (e.g., 'users_id_seq')
     *
     * @throws QueryException
     */
    public function lastInsertId(string $sequenceName): int|string;

    /**
     * Subscribe the current connection to a PostgreSQL notification channel.
     * Subsequent NOTIFY statements on `$channel` (from this or any other
     * session) will be queued for this connection until a waitForNotification()
     * call drains them. Calling listen() for a channel already being listened
     * on is a no-op.
     *
     * @throws QueryException
     */
    public function listen(string $channel): void;

    /**
     * Get the connection parameters used to establish this connection.
     */
    public function parameters(): ConnectionParameters;

    /**
     * Roll back the current transaction.
     * If nested, rolls back to the savepoint.
     *
     * @throws TransactionException
     */
    public function rollBack(): void;

    /**
     * Set auto-commit mode.
     * When enabled (default), each query is executed in its own transaction.
     * When disabled, a transaction is implicitly started and remains open until
     * commit() or rollBack() is called.
     *
     * @throws TransactionException
     */
    public function setAutoCommit(bool $autoCommit): void;

    /**
     * Execute a callback within a transaction.
     * Auto-commits on success, auto-rollbacks on exception.
     * Supports nesting via SAVEPOINTs.
     *
     * @template T
     *
     * @param callable(Client): T $callback
     *
     * @throws TransactionException
     *
     * @return T
     */
    public function transaction(callable $callback): mixed;

    /**
     * Unsubscribe the current connection from a PostgreSQL notification
     * channel. Calling unlisten() for a channel that was not being listened on
     * is a no-op. Pass a specific channel name — the `UNLISTEN *` wildcard is
     * not expressible through this API.
     *
     * @throws QueryException
     */
    public function unlisten(string $channel): void;

    /**
     * Block until a notification arrives on any channel the connection is
     * listening to, or until the timeout elapses. Returns null on timeout,
     * otherwise a Notification describing the received message.
     *
     * @param int $milliseconds Must be >= 0. A value of 0 performs a
     *                          non-blocking check and returns
     *                          immediately.
     *
     * @throws ConnectionException when the underlying socket read fails
     */
    public function wait(int $milliseconds): ?Notification;
}
