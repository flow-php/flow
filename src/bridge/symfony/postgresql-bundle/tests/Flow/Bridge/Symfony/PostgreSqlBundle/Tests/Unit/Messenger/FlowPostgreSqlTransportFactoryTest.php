<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Unit\Messenger;

use Flow\Bridge\Symfony\PostgreSqlBundle\Messenger\FlowPostgreSqlTransportFactory;
use Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Double\ArrayClientLocator;
use Flow\Bridge\Symfony\PostgreSQLMessenger\Exception\TransportException;
use Flow\Bridge\Symfony\PostgreSQLMessenger\FlowPostgreSqlTransport;
use Flow\Bridge\Symfony\PostgreSQLMessenger\Tests\Unit\Double\{FakeSerializer, SpyClient};
use PHPUnit\Framework\TestCase;

final class FlowPostgreSqlTransportFactoryTest extends TestCase
{
    public function test_create_transport_options_override_dsn_query_options() : void
    {
        $factory = new FlowPostgreSqlTransportFactory(new ArrayClientLocator(['default' => new SpyClient()]));

        $transport = $factory->createTransport(
            'flow-pgsql://default?table_name=from_dsn&queue_name=from_dsn',
            ['queue_name' => 'from_options'],
            new FakeSerializer(),
        );

        self::assertInstanceOf(FlowPostgreSqlTransport::class, $transport);
    }

    public function test_create_transport_reads_options_from_array() : void
    {
        $factory = new FlowPostgreSqlTransportFactory(new ArrayClientLocator(['main' => new SpyClient()]));

        $transport = $factory->createTransport(
            'flow-pgsql://main',
            [
                'table_name' => 'my_queue',
                'schema' => 'app',
                'queue_name' => 'high_priority',
                'redeliver_timeout' => 1800,
            ],
            new FakeSerializer(),
        );

        self::assertInstanceOf(FlowPostgreSqlTransport::class, $transport);
    }

    public function test_create_transport_reads_options_from_dsn_query_string() : void
    {
        $factory = new FlowPostgreSqlTransportFactory(new ArrayClientLocator(['default' => new SpyClient()]));

        $transport = $factory->createTransport(
            'flow-pgsql://default?table_name=from_dsn&schema=app',
            [],
            new FakeSerializer(),
        );

        self::assertInstanceOf(FlowPostgreSqlTransport::class, $transport);
    }

    public function test_create_transport_returns_transport_with_defaults() : void
    {
        $factory = new FlowPostgreSqlTransportFactory(new ArrayClientLocator(['default' => new SpyClient()]));

        $transport = $factory->createTransport('flow-pgsql://default', [], new FakeSerializer());

        self::assertInstanceOf(FlowPostgreSqlTransport::class, $transport);
    }

    public function test_create_transport_throws_on_empty_host() : void
    {
        $factory = new FlowPostgreSqlTransportFactory(new ArrayClientLocator());

        $this->expectException(TransportException::class);
        $this->expectExceptionMessage('Invalid Flow PostgreSQL Messenger DSN');

        $factory->createTransport('flow-pgsql://', [], new FakeSerializer());
    }

    public function test_create_transport_throws_when_client_not_found() : void
    {
        $factory = new FlowPostgreSqlTransportFactory(new ArrayClientLocator());

        $this->expectException(TransportException::class);
        $this->expectExceptionMessage('connection "missing" not found');

        $factory->createTransport('flow-pgsql://missing', [], new FakeSerializer());
    }

    public function test_create_transport_throws_when_service_is_not_a_client() : void
    {
        $factory = new FlowPostgreSqlTransportFactory(new ArrayClientLocator(['default' => new \stdClass()]));

        $this->expectException(TransportException::class);
        $this->expectExceptionMessage('must be an instance of');

        $factory->createTransport('flow-pgsql://default', [], new FakeSerializer());
    }

    public function test_create_transport_uses_named_connection_from_dsn() : void
    {
        $factory = new FlowPostgreSqlTransportFactory(new ArrayClientLocator([
            'primary' => new SpyClient(),
            'secondary' => new SpyClient(),
        ]));

        $transport = $factory->createTransport('flow-pgsql://secondary', [], new FakeSerializer());

        self::assertInstanceOf(FlowPostgreSqlTransport::class, $transport);
    }

    public function test_does_not_support_amqp_dsn() : void
    {
        $factory = new FlowPostgreSqlTransportFactory(new ArrayClientLocator());

        self::assertFalse($factory->supports('amqp://localhost/%2f/messages', []));
    }

    public function test_does_not_support_doctrine_dsn() : void
    {
        $factory = new FlowPostgreSqlTransportFactory(new ArrayClientLocator());

        self::assertFalse($factory->supports('doctrine://default', []));
    }

    public function test_does_not_support_plain_postgresql_dsn() : void
    {
        $factory = new FlowPostgreSqlTransportFactory(new ArrayClientLocator());

        self::assertFalse($factory->supports('postgresql://user:pass@host/db', []));
        self::assertFalse($factory->supports('pgsql://user:pass@host/db', []));
    }

    public function test_supports_flow_pgsql_dsn() : void
    {
        $factory = new FlowPostgreSqlTransportFactory(new ArrayClientLocator());

        self::assertTrue($factory->supports('flow-pgsql://default', []));
    }

    public function test_supports_flow_postgresql_dsn() : void
    {
        $factory = new FlowPostgreSqlTransportFactory(new ArrayClientLocator());

        self::assertTrue($factory->supports('flow-postgresql://default', []));
    }
}
