<?php

declare(strict_types=1);

namespace Flow\PostgreSql\DSL;

use Flow\ETL\Attribute\DocumentationDSL;
use Flow\ETL\Attribute\Module;
use Flow\ETL\Attribute\Type as DSLType;
use Flow\PostgreSql\Client;
use Flow\PostgreSql\Client\ConnectionParameters;
use Flow\PostgreSql\Client\Context;
use Flow\PostgreSql\Client\ConvertedParameters;
use Flow\PostgreSql\Client\DsnParser;
use Flow\PostgreSql\Client\Exception\ConnectionException;
use Flow\PostgreSql\Client\Infrastructure\PgSql\PgSqlClient;
use Flow\PostgreSql\Client\RowMapper;
use Flow\PostgreSql\Client\RowMapper\ConstructorMapper;
use Flow\PostgreSql\Client\RowMapper\StaticFactoryMapper;
use Flow\PostgreSql\Client\RowMapper\TypeMapper;
use Flow\PostgreSql\Client\Telemetry\PostgreSqlTelemetryConfig;
use Flow\PostgreSql\Client\Telemetry\PostgreSqlTelemetryOptions;
use Flow\PostgreSql\Client\Telemetry\TraceableClient;
use Flow\PostgreSql\Client\Telemetry\TransactionSpanMode;
use Flow\PostgreSql\Client\TypedValue;
use Flow\PostgreSql\Client\Types\ValueConverters;
use Flow\PostgreSql\Client\Types\ValueType;
use Flow\PostgreSql\Schema\Catalog;
use Flow\Telemetry\Telemetry;
use Flow\Types\Type as FlowType;
use Psr\Clock\ClockInterface;
use SensitiveParameter;

/**
 * Create connection parameters from a connection string.
 *
 * Accepts libpq-style connection strings:
 * - Key-value format: "host=localhost port=5432 dbname=mydb user=myuser password=secret"
 * - URI format: "postgresql://user:password@localhost:5432/dbname"
 *
 * @example
 * $params = pgsql_connection('host=localhost dbname=mydb');
 * $params = pgsql_connection('postgresql://user:pass@localhost/mydb');
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pgsql_connection(#[SensitiveParameter] string $connectionString): ConnectionParameters
{
    return ConnectionParameters::fromString($connectionString);
}

/**
 * Create connection parameters from a DSN string.
 *
 * Parses standard PostgreSQL DSN format commonly used in environment variables
 * (e.g., DATABASE_URL). Supports postgres://, postgresql://, and pgsql:// schemes.
 *
 * @param string $dsn DSN string in format: postgres://user:password@host:port/database?options
 *
 * @throws Client\DsnParserException If the DSN cannot be parsed
 *
 * @example
 * $params = pgsql_connection_dsn('postgres://myuser:secret@localhost:5432/mydb');
 * $params = pgsql_connection_dsn('postgresql://user:pass@db.example.com/app?sslmode=require');
 * $params = pgsql_connection_dsn('pgsql://user:pass@localhost/mydb'); // Symfony/Doctrine format
 * $params = pgsql_connection_dsn(getenv('DATABASE_URL'));
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pgsql_connection_dsn(#[SensitiveParameter] string $dsn): ConnectionParameters
{
    return (new DsnParser())->parse($dsn);
}

/**
 * Create connection parameters from individual values.
 *
 * Allows specifying connection parameters individually for better type safety
 * and IDE support.
 *
 * @param string $database Database name (required)
 * @param string $host Hostname (default: localhost)
 * @param int $port Port number (default: 5432)
 * @param null|string $user Username (optional)
 * @param null|string $password Password (optional)
 * @param array<string, string> $options Additional libpq options
 *
 * @example
 * $params = pgsql_connection_params(
 *     database: 'mydb',
 *     host: 'localhost',
 *     user: 'myuser',
 *     password: 'secret',
 * );
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pgsql_connection_params(
    string $database,
    string $host = 'localhost',
    int $port = 5432,
    ?string $user = null,
    #[SensitiveParameter]
    ?string $password = null,
    array $options = [],
): ConnectionParameters {
    return ConnectionParameters::fromParams(
        database: $database,
        host: $host,
        port: $port,
        user: $user,
        password: $password,
        options: $options,
    );
}

/**
 * Create a PostgreSQL client using ext-pgsql.
 *
 * The client connects immediately and is ready to execute queries.
 *
 * @param Client\ConnectionParameters $params Connection parameters
 * @param null|ValueConverters $valueConverters Custom type converters (optional)
 * @param null|Context $context Base mapper Context — the Client enriches it with sql/parameters/self per query before handing it to RowMapper::map()
 *
 * @throws ConnectionException If connection fails
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pgsql_client(
    ConnectionParameters $params,
    ?ValueConverters $valueConverters = null,
    ?Context $context = null,
): Client\Client {
    return PgSqlClient::connect($params, $valueConverters, $context);
}

/**
 * Create a RowMapper Context seeded with user-supplied key/value data and an optional Catalog.
 *
 * The Context is later enriched with a Query (sql + parameters) and the executing Client by the
 * PostgreSQL Client before being handed to RowMapper::map().
 *
 * @param array<string, mixed> $data User-supplied key/value pairs
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function postgresql_context(array $data = [], ?Catalog $catalog = null): Context
{
    return new Context(catalog: $catalog, data: $data);
}

/**
 * Create telemetry options for PostgreSQL client instrumentation.
 *
 * Controls which telemetry signals (traces, metrics, logs) are enabled
 * and how query information is captured.
 *
 * @param bool $traceQueries Create spans for query execution (default: true)
 * @param TransactionSpanMode $transactionSpans How transactions are traced: GROUPED (default), PER_OPERATION or OFF
 * @param bool $collectMetrics Collect duration and row count metrics (default: true)
 * @param bool $logQueries Log executed queries (default: false)
 * @param null|int $maxQueryLength Maximum query text length in telemetry (default: 1000, null = unlimited)
 * @param bool $includeParameters Include query parameters in telemetry (default: false, security consideration)
 *
 * @example
 * // Default options (traces and metrics enabled)
 * $options = postgresql_telemetry_options();
 *
 * // Enable query logging
 * $options = postgresql_telemetry_options(logQueries: true);
 *
 * // Metrics only, no spans
 * $options = postgresql_telemetry_options(
 *     traceQueries: false,
 *     transactionSpans: TransactionSpanMode::OFF,
 *     collectMetrics: true,
 * );
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function postgresql_telemetry_options(
    bool $traceQueries = true,
    TransactionSpanMode $transactionSpans = TransactionSpanMode::GROUPED,
    bool $collectMetrics = true,
    bool $logQueries = false,
    ?int $maxQueryLength = 1000,
    bool $includeParameters = false,
    ?int $maxParameters = 10,
    ?int $maxParameterLength = 100,
): PostgreSqlTelemetryOptions {
    return new PostgreSqlTelemetryOptions(
        traceQueries: $traceQueries,
        transactionSpans: $transactionSpans,
        collectMetrics: $collectMetrics,
        logQueries: $logQueries,
        maxQueryLength: $maxQueryLength,
        includeParameters: $includeParameters,
        maxParameters: $maxParameters,
        maxParameterLength: $maxParameterLength,
    );
}

/**
 * Create telemetry configuration for PostgreSQL client.
 *
 * Bundles telemetry instance, clock, and options needed to instrument a PostgreSQL client.
 *
 * @param Telemetry $telemetry The telemetry instance
 * @param ClockInterface $clock Clock for timestamps
 * @param null|PostgreSqlTelemetryOptions $options Telemetry options (default: all enabled)
 *
 * @example
 * $config = postgresql_telemetry_config(
 *     telemetry(resource(['service.name' => 'my-app'])),
 *     new SystemClock(),
 * );
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function postgresql_telemetry_config(
    Telemetry $telemetry,
    ClockInterface $clock,
    ?PostgreSqlTelemetryOptions $options = null,
): PostgreSqlTelemetryConfig {
    return new PostgreSqlTelemetryConfig(
        telemetry: $telemetry,
        clock: $clock,
        options: $options ?? new PostgreSqlTelemetryOptions(),
    );
}

/**
 * Wrap a PostgreSQL client with telemetry instrumentation.
 *
 * Returns a decorator that adds spans, metrics, and logs to all
 * query and transaction operations following OpenTelemetry conventions.
 *
 * @param Client\Client $client The PostgreSQL client to instrument
 * @param PostgreSqlTelemetryConfig $telemetryConfig Telemetry configuration
 *
 * @example
 * $client = pgsql_client(pgsql_connection('host=localhost dbname=mydb'));
 *
 * $traceableClient = traceable_postgresql_client(
 *     $client,
 *     postgresql_telemetry_config(
 *         telemetry(resource(['service.name' => 'my-app'])),
 *         new SystemClock(),
 *         postgresql_telemetry_options(
 *             traceQueries: true,
 *             transactionSpans: TransactionSpanMode::GROUPED,
 *             collectMetrics: true,
 *             logQueries: true,
 *             maxQueryLength: 500,
 *         ),
 *     ),
 * );
 *
 * // All operations now traced
 * $traceableClient->transaction(function (Client $client) {
 *     $user = $client->fetchSingle('SELECT * FROM users WHERE id = $1', [123]);
 *     $client->execute('UPDATE users SET last_login = NOW() WHERE id = $1', [123]);
 * });
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function traceable_postgresql_client(Client\Client $client, PostgreSqlTelemetryConfig $telemetryConfig): TraceableClient
{
    return new TraceableClient($client, $telemetryConfig);
}

/**
 * Create a default constructor-based row mapper.
 *
 * Maps database rows directly to constructor parameters.
 * Column names must match parameter names exactly (1:1).
 * Use SQL aliases if column names differ from parameter names.
 *
 * @example
 * // DTO where column names match parameter names
 * readonly class User {
 *     public function __construct(
 *         public int $id,
 *         public string $name,
 *         public string $email,
 *     ) {}
 * }
 *
 * // Usage
 * $client = pgsql_client(pgsql_connection('...'), mapper: pgsql_mapper());
 *
 * // For snake_case columns, use SQL aliases
 * $user = $client->fetchInto(
 *     User::class,
 *     'SELECT id, user_name AS name, user_email AS email FROM users WHERE id = $1',
 *     [1]
 * );
 */
/**
 * @template T of object
 *
 * @param class-string<T> $class
 *
 * @return ConstructorMapper<T>
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function constructor_mapper(string $class): ConstructorMapper
{
    return new ConstructorMapper($class);
}

/**
 * @template TType
 * @template TOut
 *
 * @param FlowType<TType> $type
 * @param null|RowMapper<TOut> $next
 *
 * @return ($next is null ? TypeMapper<TType, TType> : TypeMapper<TType, TOut>)
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function type_mapper(FlowType $type, ?RowMapper $next = null): TypeMapper
{
    if ($next === null) {
        // Mago 1.26+ already infers TypeMapper<TType, TType> from the conditional
        // @return tag, but PHPStan needs this @var to bind the second template.
        // @mago-expect analysis:redundant-docblock-type
        /** @var TypeMapper<TType, TType> */
        return new TypeMapper($type);
    }

    return new TypeMapper($type, $next);
}

/**
 * Create a row mapper backed by a public static factory method.
 *
 * The factory method must accept a single array<string, mixed> $row and return
 * an instance of the target class. If your factory needs access to the mapping
 * Context (sql/parameters/client/catalog/user-data), implement RowMapper directly.
 *
 * @template T of object
 *
 * @param class-string<T> $class
 * @param non-empty-string $method
 *
 * @return StaticFactoryMapper<T>
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function static_factory_mapper(string $class, string $method): StaticFactoryMapper
{
    return new StaticFactoryMapper($class, $method);
}

/**
 * Wrap a value with explicit PostgreSQL type information for parameter binding.
 *
 * Use when auto-detection isn't sufficient or when you need to specify
 * the exact PostgreSQL type (since one PHP type can map to multiple PostgreSQL types):
 * - int could be INT2, INT4, or INT8
 * - string could be TEXT, VARCHAR, or CHAR
 * - array must always use typed() since auto-detection cannot determine element type
 * - DateTimeInterface could be TIMESTAMP or TIMESTAMPTZ
 * - Json could be JSON or JSONB
 *
 * @param mixed $value The value to bind
 * @param ValueType $targetType The PostgreSQL type to convert the value to
 *
 * @example
 * $client->fetch(
 *     'SELECT * FROM users WHERE id = $1 AND tags = $2',
 *     [
 *         typed('550e8400-e29b-41d4-a716-446655440000', ValueType::UUID),
 *         typed(['tag1', 'tag2'], ValueType::TEXT_ARRAY),
 *     ]
 * );
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function typed(mixed $value, ValueType $targetType): TypedValue
{
    return new TypedValue($value, $targetType);
}

/**
 * Parameters already in PostgreSQL's text form, which Client::execute() sends without running a converter.
 *
 * @param list<null|string> $values
 *
 * @example
 * $client->execute('UPDATE users SET active = $1 WHERE id = $2', converted_parameters(['f', '1']));
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function converted_parameters(array $values): ConvertedParameters
{
    return new ConvertedParameters($values);
}
