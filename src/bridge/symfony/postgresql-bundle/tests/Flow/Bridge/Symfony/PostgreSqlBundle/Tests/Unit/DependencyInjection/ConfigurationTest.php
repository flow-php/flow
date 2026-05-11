<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Unit\DependencyInjection;

use Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Context\ConfigurationContext;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

final class ConfigurationTest extends TestCase
{
    private ConfigurationContext $context;

    protected function setUp(): void
    {
        $this->context = new ConfigurationContext();
    }

    public function test_cache_pool_connection_can_be_null(): void
    {
        $config = $this->context->processConfig([
            'connections' => [
                'default' => ['dsn' => 'postgresql://user:pass@localhost:5432/db'],
            ],
            'cache' => [
                'pools' => [
                    'app' => [],
                ],
            ],
        ]);

        static::assertNull($config['cache']['pools']['app']['connection']);
    }

    public function test_cache_pool_share_connection_can_be_enabled(): void
    {
        $config = $this->context->processConfig([
            'connections' => [
                'default' => ['dsn' => 'postgresql://user:pass@localhost:5432/db'],
            ],
            'cache' => [
                'pools' => [
                    'app' => ['share_connection' => true],
                ],
            ],
        ]);

        static::assertTrue($config['cache']['pools']['app']['share_connection']);
    }

    public function test_cache_pools_custom_columns_and_namespace(): void
    {
        $config = $this->context->processConfig([
            'connections' => [
                'default' => ['dsn' => 'postgresql://user:pass@localhost:5432/db'],
            ],
            'cache' => [
                'pools' => [
                    'sessions' => [
                        'connection' => 'default',
                        'table_name' => 'app_cache',
                        'schema' => 'caching',
                        'id_col' => 'k',
                        'data_col' => 'v',
                        'lifetime_col' => 'ttl',
                        'time_col' => 'ts',
                        'namespace' => 'sess.',
                        'default_lifetime' => 3600,
                        'marshaller_service_id' => 'app.marshaller',
                    ],
                ],
            ],
        ]);

        $pool = $config['cache']['pools']['sessions'];
        static::assertSame('default', $pool['connection']);
        static::assertSame('app_cache', $pool['table_name']);
        static::assertSame('caching', $pool['schema']);
        static::assertSame('k', $pool['id_col']);
        static::assertSame('v', $pool['data_col']);
        static::assertSame('ttl', $pool['lifetime_col']);
        static::assertSame('ts', $pool['time_col']);
        static::assertSame('sess.', $pool['namespace']);
        static::assertSame(3600, $pool['default_lifetime']);
        static::assertSame('app.marshaller', $pool['marshaller_service_id']);
    }

    public function test_cache_pools_default_table_and_schema(): void
    {
        $config = $this->context->processConfig([
            'connections' => [
                'default' => ['dsn' => 'postgresql://user:pass@localhost:5432/db'],
            ],
            'cache' => [
                'pools' => [
                    'app' => [],
                ],
            ],
        ]);

        $pool = $config['cache']['pools']['app'];
        static::assertSame('cache_items', $pool['table_name']);
        static::assertSame('public', $pool['schema']);
        static::assertSame('item_id', $pool['id_col']);
        static::assertSame('item_data', $pool['data_col']);
        static::assertSame('item_lifetime', $pool['lifetime_col']);
        static::assertSame('item_time', $pool['time_col']);
        static::assertSame('', $pool['namespace']);
        static::assertSame(0, $pool['default_lifetime']);
        static::assertNull($pool['marshaller_service_id']);
        static::assertFalse($pool['share_connection']);
    }

    public function test_cache_section_can_be_omitted(): void
    {
        $config = $this->context->processConfig([
            'connections' => [
                'default' => ['dsn' => 'postgresql://user:pass@localhost:5432/db'],
            ],
        ]);

        static::assertSame([], $config['cache']['pools']);
    }

    public function test_catalog_providers_at_top_level_with_inline_catalog(): void
    {
        $catalogData = [
            'schemas' => [
                [
                    'name' => 'public',
                    'tables' => [
                        [
                            'name' => 'users',
                            'columns' => [
                                [
                                    'name' => 'id',
                                    'type' => ['name' => 'int4', 'schema' => 'pg_catalog'],
                                    'nullable' => false,
                                ],
                            ],
                        ],
                    ],
                ],
            ],
        ];

        $config = $this->context->processConfig([
            'connections' => [
                'default' => [
                    'dsn' => 'postgresql://user:pass@localhost:5432/db',
                ],
            ],
            'migrations' => [
                'enabled' => true,
            ],
            'catalog_providers' => [
                ['catalog' => $catalogData],
            ],
        ]);

        static::assertCount(1, $config['catalog_providers']);
        static::assertSame($catalogData, $config['catalog_providers'][0]['catalog']);
    }

    public function test_catalog_providers_at_top_level_with_service_reference(): void
    {
        $config = $this->context->processConfig([
            'connections' => [
                'default' => [
                    'dsn' => 'postgresql://user:pass@localhost:5432/db',
                ],
            ],
            'migrations' => [
                'enabled' => true,
                'directory' => '/custom/migrations',
                'namespace' => 'Custom\\Migrations',
                'table_name' => 'custom_migrations',
                'table_schema' => 'custom',
            ],
            'catalog_providers' => [
                ['catalog_provider_id' => 'app.catalog_provider'],
            ],
        ]);

        static::assertSame('app.catalog_provider', $config['catalog_providers'][0]['catalog_provider_id']);
        static::assertSame('/custom/migrations', $config['migrations']['directory']);
        static::assertSame('Custom\\Migrations', $config['migrations']['namespace']);
        static::assertSame('custom_migrations', $config['migrations']['table_name']);
        static::assertSame('custom', $config['migrations']['table_schema']);
    }

    public function test_catalog_providers_multiple_entries(): void
    {
        $config = $this->context->processConfig([
            'connections' => [
                'default' => [
                    'dsn' => 'postgresql://user:pass@localhost:5432/db',
                ],
            ],
            'catalog_providers' => [
                ['catalog' => ['schemas' => []]],
                ['catalog_provider_id' => 'app.second_provider'],
            ],
        ]);

        static::assertCount(2, $config['catalog_providers']);
        static::assertSame('app.second_provider', $config['catalog_providers'][1]['catalog_provider_id']);
    }

    public function test_connections_requires_at_least_one_element(): void
    {
        $this->expectException(InvalidConfigurationException::class);

        $this->context->processConfig([
            'connections' => [],
        ]);
    }

    public function test_context_accepts_arbitrary_variables(): void
    {
        $config = $this->context->processConfig([
            'connections' => [
                'default' => [
                    'dsn' => 'postgresql://user:pass@localhost:5432/db',
                    'context' => [
                        'tenant_id' => 42,
                        'logger' => '@my.logger',
                        'ttl' => '%cache.ttl%',
                        'db_url' => '%env(DATABASE_URL)%',
                        'tags' => ['a', 'b'],
                    ],
                ],
            ],
        ]);

        static::assertSame(
            [
                'tenant_id' => 42,
                'logger' => '@my.logger',
                'ttl' => '%cache.ttl%',
                'db_url' => '%env(DATABASE_URL)%',
                'tags' => ['a', 'b'],
            ],
            $config['connections']['default']['context'],
        );
    }

    public function test_context_defaults_to_empty(): void
    {
        $config = $this->context->processConfig([
            'connections' => [
                'default' => [
                    'dsn' => 'postgresql://user:pass@localhost:5432/db',
                ],
            ],
        ]);

        static::assertSame([], $config['connections']['default']['context']);
    }

    public function test_dsn_is_required(): void
    {
        $this->expectException(InvalidConfigurationException::class);

        $this->context->processConfig([
            'connections' => [
                'default' => [],
            ],
        ]);
    }

    public function test_messenger_custom_table_and_schema(): void
    {
        $config = $this->context->processConfig([
            'connections' => [
                'default' => ['dsn' => 'postgresql://user:pass@localhost:5432/db'],
            ],
            'messenger' => [
                'enabled' => true,
                'table_name' => 'custom_queue',
                'schema' => 'app',
            ],
        ]);

        static::assertTrue($config['messenger']['enabled']);
        static::assertSame('custom_queue', $config['messenger']['table_name']);
        static::assertSame('app', $config['messenger']['schema']);
    }

    public function test_messenger_defaults(): void
    {
        $config = $this->context->processConfig([
            'connections' => [
                'default' => ['dsn' => 'postgresql://user:pass@localhost:5432/db'],
            ],
            'messenger' => [
                'enabled' => true,
            ],
        ]);

        static::assertTrue($config['messenger']['enabled']);
        static::assertSame('messenger_messages', $config['messenger']['table_name']);
        static::assertSame('public', $config['messenger']['schema']);
    }

    public function test_messenger_disabled_by_default(): void
    {
        $config = $this->context->processConfig([
            'connections' => [
                'default' => ['dsn' => 'postgresql://user:pass@localhost:5432/db'],
            ],
        ]);

        static::assertFalse($config['messenger']['enabled']);
    }

    public function test_migrations_default_values(): void
    {
        $config = $this->context->processConfig([
            'connections' => [
                'default' => [
                    'dsn' => 'postgresql://user:pass@localhost:5432/db',
                ],
            ],
            'migrations' => [
                'enabled' => true,
            ],
        ]);

        static::assertSame('%kernel.project_dir%/migrations', $config['migrations']['directory']);
        static::assertSame('App\\Migrations', $config['migrations']['namespace']);
        static::assertSame('flow_migrations', $config['migrations']['table_name']);
        static::assertSame('public', $config['migrations']['table_schema']);
        static::assertFalse($config['migrations']['all_or_nothing']);
        static::assertTrue($config['migrations']['generate_rollback']);
    }

    public function test_migrations_enabled_without_catalog_providers_is_valid_at_config_level(): void
    {
        $config = $this->context->processConfig([
            'connections' => [
                'default' => [
                    'dsn' => 'postgresql://user:pass@localhost:5432/db',
                ],
            ],
            'migrations' => [
                'enabled' => true,
            ],
        ]);

        static::assertTrue($config['migrations']['enabled']);
    }

    public function test_multiple_connections(): void
    {
        $config = $this->context->processConfig([
            'connections' => [
                'default' => [
                    'dsn' => 'postgresql://user:pass@localhost:5432/db1',
                ],
                'analytics' => [
                    'dsn' => 'postgresql://user:pass@localhost:5432/db2',
                ],
            ],
        ]);

        static::assertArrayHasKey('default', $config['connections']);
        static::assertArrayHasKey('analytics', $config['connections']);
        static::assertSame('postgresql://user:pass@localhost:5432/db1', $config['connections']['default']['dsn']);
        static::assertSame('postgresql://user:pass@localhost:5432/db2', $config['connections']['analytics']['dsn']);
    }

    public function test_session_default_disabled(): void
    {
        $config = $this->context->processConfig([
            'connections' => [
                'default' => ['dsn' => 'postgresql://user:pass@localhost:5432/db'],
            ],
        ]);

        static::assertFalse($config['session']['enabled']);
    }

    public function test_session_defaults_when_enabled(): void
    {
        $config = $this->context->processConfig([
            'connections' => [
                'default' => ['dsn' => 'postgresql://user:pass@localhost:5432/db'],
            ],
            'session' => [
                'enabled' => true,
            ],
        ]);

        $session = $config['session'];
        static::assertTrue($session['enabled']);
        static::assertNull($session['connection']);
        static::assertSame('sessions', $session['table_name']);
        static::assertSame('public', $session['schema']);
        static::assertSame('sess_id', $session['id_col']);
        static::assertSame('sess_data', $session['data_col']);
        static::assertSame('sess_lifetime', $session['lifetime_col']);
        static::assertSame('sess_time', $session['time_col']);
        static::assertSame('transactional', $session['lock_mode']);
        static::assertNull($session['ttl']);
        static::assertFalse($session['share_connection']);
    }

    public function test_session_lock_mode_rejects_invalid_value(): void
    {
        $this->expectException(InvalidConfigurationException::class);

        $this->context->processConfig([
            'connections' => [
                'default' => ['dsn' => 'postgresql://user:pass@localhost:5432/db'],
            ],
            'session' => [
                'enabled' => true,
                'lock_mode' => 'pessimistic',
            ],
        ]);
    }

    public function test_session_overrides(): void
    {
        $config = $this->context->processConfig([
            'connections' => [
                'default' => ['dsn' => 'postgresql://user:pass@localhost:5432/db'],
            ],
            'session' => [
                'enabled' => true,
                'connection' => 'default',
                'table_name' => 'app_sessions',
                'schema' => 'sess',
                'id_col' => 'sid',
                'data_col' => 'sdata',
                'lifetime_col' => 'sttl',
                'time_col' => 'sts',
                'lock_mode' => 'advisory',
                'ttl' => 7200,
            ],
        ]);

        $session = $config['session'];
        static::assertSame('default', $session['connection']);
        static::assertSame('app_sessions', $session['table_name']);
        static::assertSame('sess', $session['schema']);
        static::assertSame('sid', $session['id_col']);
        static::assertSame('sdata', $session['data_col']);
        static::assertSame('sttl', $session['lifetime_col']);
        static::assertSame('sts', $session['time_col']);
        static::assertSame('advisory', $session['lock_mode']);
        static::assertSame(7200, $session['ttl']);
    }

    public function test_session_share_connection_can_be_enabled(): void
    {
        $config = $this->context->processConfig([
            'connections' => [
                'default' => ['dsn' => 'postgresql://user:pass@localhost:5432/db'],
            ],
            'session' => [
                'enabled' => true,
                'share_connection' => true,
            ],
        ]);

        static::assertTrue($config['session']['share_connection']);
    }

    public function test_session_ttl_rejects_negative(): void
    {
        $this->expectException(InvalidConfigurationException::class);

        $this->context->processConfig([
            'connections' => [
                'default' => ['dsn' => 'postgresql://user:pass@localhost:5432/db'],
            ],
            'session' => [
                'enabled' => true,
                'ttl' => -1,
            ],
        ]);
    }

    public function test_single_connection_with_defaults(): void
    {
        $config = $this->context->processConfig([
            'connections' => [
                'default' => [
                    'dsn' => 'postgresql://user:pass@localhost:5432/db',
                ],
            ],
        ]);

        static::assertSame('postgresql://user:pass@localhost:5432/db', $config['connections']['default']['dsn']);
        static::assertFalse($config['migrations']['enabled']);
    }

    public function test_telemetry_not_present_by_default(): void
    {
        $config = $this->context->processConfig([
            'connections' => [
                'default' => [
                    'dsn' => 'postgresql://user:pass@localhost:5432/db',
                ],
            ],
        ]);

        static::assertArrayNotHasKey('telemetry', $config['connections']['default']);
    }

    public function test_telemetry_requires_service_id(): void
    {
        $this->expectException(InvalidConfigurationException::class);

        $this->context->processConfig([
            'connections' => [
                'default' => [
                    'dsn' => 'postgresql://user:pass@localhost:5432/db',
                    'telemetry' => [],
                ],
            ],
        ]);
    }

    public function test_telemetry_with_custom_options(): void
    {
        $config = $this->context->processConfig([
            'connections' => [
                'default' => [
                    'dsn' => 'postgresql://user:pass@localhost:5432/db',
                    'telemetry' => [
                        'service_id' => 'my.telemetry',
                        'clock_service_id' => 'my.clock',
                        'trace_queries' => false,
                        'trace_transactions' => false,
                        'collect_metrics' => false,
                        'log_queries' => true,
                        'max_query_length' => 500,
                        'include_parameters' => true,
                        'max_parameters' => 5,
                        'max_parameter_length' => 50,
                    ],
                ],
            ],
        ]);

        $telemetry = $config['connections']['default']['telemetry'];
        static::assertSame('my.telemetry', $telemetry['service_id']);
        static::assertSame('my.clock', $telemetry['clock_service_id']);
        static::assertFalse($telemetry['trace_queries']);
        static::assertFalse($telemetry['trace_transactions']);
        static::assertFalse($telemetry['collect_metrics']);
        static::assertTrue($telemetry['log_queries']);
        static::assertSame(500, $telemetry['max_query_length']);
        static::assertTrue($telemetry['include_parameters']);
        static::assertSame(5, $telemetry['max_parameters']);
        static::assertSame(50, $telemetry['max_parameter_length']);
    }

    public function test_telemetry_with_defaults(): void
    {
        $config = $this->context->processConfig([
            'connections' => [
                'default' => [
                    'dsn' => 'postgresql://user:pass@localhost:5432/db',
                    'telemetry' => [
                        'service_id' => 'flow.telemetry',
                    ],
                ],
            ],
        ]);

        $telemetry = $config['connections']['default']['telemetry'];
        static::assertSame('flow.telemetry', $telemetry['service_id']);
        static::assertNull($telemetry['clock_service_id']);
        static::assertTrue($telemetry['trace_queries']);
        static::assertTrue($telemetry['trace_transactions']);
        static::assertTrue($telemetry['collect_metrics']);
        static::assertFalse($telemetry['log_queries']);
        static::assertSame(1000, $telemetry['max_query_length']);
        static::assertFalse($telemetry['include_parameters']);
        static::assertSame(10, $telemetry['max_parameters']);
        static::assertSame(100, $telemetry['max_parameter_length']);
    }
}
