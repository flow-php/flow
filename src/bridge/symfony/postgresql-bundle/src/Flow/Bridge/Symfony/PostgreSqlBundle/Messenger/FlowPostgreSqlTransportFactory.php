<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Messenger;

use Flow\Bridge\Symfony\PostgreSQLMessenger\Connection;
use Flow\Bridge\Symfony\PostgreSQLMessenger\Exception\TransportException;
use Flow\Bridge\Symfony\PostgreSQLMessenger\FlowPostgreSqlTransport;
use Flow\PostgreSql\Client\Client;
use Psr\Container\ContainerInterface;
use Psr\Container\NotFoundExceptionInterface;
use SensitiveParameter;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportFactoryInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;

use function array_key_exists;
use function array_replace;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function get_debug_type;
use function parse_str;
use function parse_url;
use function sprintf;
use function str_starts_with;

/**
 * @implements TransportFactoryInterface<FlowPostgreSqlTransport>
 */
final readonly class FlowPostgreSqlTransportFactory implements TransportFactoryInterface
{
    public function __construct(
        private ContainerInterface $clients,
    ) {}

    /**
     * @param array<string, mixed> $options
     */
    public function createTransport(
        #[SensitiveParameter]
        string $dsn,
        array $options,
        SerializerInterface $serializer,
    ): TransportInterface {
        $parsed = parse_url($dsn);

        if ($parsed === false || !array_key_exists('host', $parsed) || $parsed['host'] === '') {
            throw new TransportException(sprintf(
                'Invalid Flow PostgreSQL Messenger DSN "%s": expected "flow-pgsql://<connection_name>".',
                $dsn,
            ));
        }

        $connectionName = $parsed['host'];

        $dsnOptions = [];

        if (array_key_exists('query', $parsed) && $parsed['query'] !== '') {
            parse_str($parsed['query'], $dsnOptions);
        }

        $merged = array_replace($dsnOptions, $options);

        try {
            $client = $this->clients->get($connectionName);
        } catch (NotFoundExceptionInterface $e) {
            throw new TransportException(
                sprintf(
                    'Flow PostgreSQL Messenger connection "%s" not found. Make sure it is registered under flow_postgresql.connections.',
                    $connectionName,
                ),
                0,
                $e,
            );
        }

        if (!$client instanceof Client) {
            throw new TransportException(sprintf(
                'Flow PostgreSQL Messenger connection "%s" must be an instance of %s, got %s.',
                $connectionName,
                Client::class,
                get_debug_type($client),
            ));
        }

        $connection = new Connection(
            client: $client,
            tableName: array_key_exists('table_name', $merged)
                ? type_string()->assert($merged['table_name'])
                : 'messenger_messages',
            schemaName: array_key_exists('schema', $merged) ? type_string()->assert($merged['schema']) : 'public',
            queueName: array_key_exists('queue_name', $merged)
                ? type_string()->assert($merged['queue_name'])
                : 'default',
            redeliverTimeout: array_key_exists('redeliver_timeout', $merged)
                ? type_integer()->assert($merged['redeliver_timeout'])
                : 3600,
        );

        return new FlowPostgreSqlTransport($connection, $serializer);
    }

    /**
     * @param array<string, mixed> $options
     */
    public function supports(#[SensitiveParameter] string $dsn, array $options): bool
    {
        return str_starts_with($dsn, 'flow-pgsql://') || str_starts_with($dsn, 'flow-postgresql://');
    }
}
