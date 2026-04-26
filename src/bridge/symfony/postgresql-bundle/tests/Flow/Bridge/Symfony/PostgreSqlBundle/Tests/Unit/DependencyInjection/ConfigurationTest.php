<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Unit\DependencyInjection;

use Flow\Bridge\Symfony\PostgreSqlBundle\DependencyInjection\Configuration;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;
use Symfony\Component\Config\Definition\Processor;

final class ConfigurationTest extends TestCase
{
    public function test_cache_pool_connection_can_be_null() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'connections' => [
                'default' => ['dsn' => 'postgresql://user:pass@localhost:5432/db'],
            ],
            'cache' => [
                'pools' => [
                    'app' => [],
                ],
            ],
        ]]);

        self::assertNull($config['cache']['pools']['app']['connection']);
    }

    public function test_cache_pools_custom_columns_and_namespace() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
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
        ]]);

        $pool = $config['cache']['pools']['sessions'];
        self::assertSame('default', $pool['connection']);
        self::assertSame('app_cache', $pool['table_name']);
        self::assertSame('caching', $pool['schema']);
        self::assertSame('k', $pool['id_col']);
        self::assertSame('v', $pool['data_col']);
        self::assertSame('ttl', $pool['lifetime_col']);
        self::assertSame('ts', $pool['time_col']);
        self::assertSame('sess.', $pool['namespace']);
        self::assertSame(3600, $pool['default_lifetime']);
        self::assertSame('app.marshaller', $pool['marshaller_service_id']);
    }

    public function test_cache_pools_default_table_and_schema() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'connections' => [
                'default' => ['dsn' => 'postgresql://user:pass@localhost:5432/db'],
            ],
            'cache' => [
                'pools' => [
                    'app' => [],
                ],
            ],
        ]]);

        $pool = $config['cache']['pools']['app'];
        self::assertSame('cache_items', $pool['table_name']);
        self::assertSame('public', $pool['schema']);
        self::assertSame('item_id', $pool['id_col']);
        self::assertSame('item_data', $pool['data_col']);
        self::assertSame('item_lifetime', $pool['lifetime_col']);
        self::assertSame('item_time', $pool['time_col']);
        self::assertSame('', $pool['namespace']);
        self::assertSame(0, $pool['default_lifetime']);
        self::assertNull($pool['marshaller_service_id']);
    }

    public function test_cache_section_can_be_omitted() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'connections' => [
                'default' => ['dsn' => 'postgresql://user:pass@localhost:5432/db'],
            ],
        ]]);

        self::assertSame([], $config['cache']['pools']);
    }

    public function test_catalog_providers_at_top_level_with_inline_catalog() : void
    {
        $catalogData = [
            'schemas' => [
                [
                    'name' => 'public',
                    'tables' => [
                        [
                            'name' => 'users',
                            'columns' => [
                                ['name' => 'id', 'type' => ['name' => 'int4', 'schema' => 'pg_catalog'], 'nullable' => false],
                            ],
                        ],
                    ],
                ],
            ],
        ];

        $config = (new Processor())->processConfiguration(new Configuration(), [[
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
        ]]);

        self::assertCount(1, $config['catalog_providers']);
        self::assertSame($catalogData, $config['catalog_providers'][0]['catalog']);
    }

    public function test_catalog_providers_at_top_level_with_service_reference() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
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
        ]]);

        self::assertSame('app.catalog_provider', $config['catalog_providers'][0]['catalog_provider_id']);
        self::assertSame('/custom/migrations', $config['migrations']['directory']);
        self::assertSame('Custom\\Migrations', $config['migrations']['namespace']);
        self::assertSame('custom_migrations', $config['migrations']['table_name']);
        self::assertSame('custom', $config['migrations']['table_schema']);
    }

    public function test_catalog_providers_multiple_entries() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'connections' => [
                'default' => [
                    'dsn' => 'postgresql://user:pass@localhost:5432/db',
                ],
            ],
            'catalog_providers' => [
                ['catalog' => ['schemas' => []]],
                ['catalog_provider_id' => 'app.second_provider'],
            ],
        ]]);

        self::assertCount(2, $config['catalog_providers']);
        self::assertSame('app.second_provider', $config['catalog_providers'][1]['catalog_provider_id']);
    }

    public function test_connections_requires_at_least_one_element() : void
    {
        $this->expectException(InvalidConfigurationException::class);

        (new Processor())->processConfiguration(new Configuration(), [[
            'connections' => [],
        ]]);
    }

    public function test_context_accepts_arbitrary_variables() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
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
        ]]);

        self::assertSame([
            'tenant_id' => 42,
            'logger' => '@my.logger',
            'ttl' => '%cache.ttl%',
            'db_url' => '%env(DATABASE_URL)%',
            'tags' => ['a', 'b'],
        ], $config['connections']['default']['context']);
    }

    public function test_context_defaults_to_empty() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'connections' => [
                'default' => [
                    'dsn' => 'postgresql://user:pass@localhost:5432/db',
                ],
            ],
        ]]);

        self::assertSame([], $config['connections']['default']['context']);
    }

    public function test_dsn_is_required() : void
    {
        $this->expectException(InvalidConfigurationException::class);

        (new Processor())->processConfiguration(new Configuration(), [[
            'connections' => [
                'default' => [],
            ],
        ]]);
    }

    public function test_messenger_custom_table_and_schema() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'connections' => [
                'default' => ['dsn' => 'postgresql://user:pass@localhost:5432/db'],
            ],
            'messenger' => [
                'enabled' => true,
                'table_name' => 'custom_queue',
                'schema' => 'app',
            ],
        ]]);

        self::assertTrue($config['messenger']['enabled']);
        self::assertSame('custom_queue', $config['messenger']['table_name']);
        self::assertSame('app', $config['messenger']['schema']);
    }

    public function test_messenger_defaults() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'connections' => [
                'default' => ['dsn' => 'postgresql://user:pass@localhost:5432/db'],
            ],
            'messenger' => [
                'enabled' => true,
            ],
        ]]);

        self::assertTrue($config['messenger']['enabled']);
        self::assertSame('messenger_messages', $config['messenger']['table_name']);
        self::assertSame('public', $config['messenger']['schema']);
    }

    public function test_messenger_disabled_by_default() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'connections' => [
                'default' => ['dsn' => 'postgresql://user:pass@localhost:5432/db'],
            ],
        ]]);

        self::assertFalse($config['messenger']['enabled']);
    }

    public function test_migrations_default_values() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'connections' => [
                'default' => [
                    'dsn' => 'postgresql://user:pass@localhost:5432/db',
                ],
            ],
            'migrations' => [
                'enabled' => true,
            ],
        ]]);

        self::assertSame('%kernel.project_dir%/migrations', $config['migrations']['directory']);
        self::assertSame('App\\Migrations', $config['migrations']['namespace']);
        self::assertSame('flow_migrations', $config['migrations']['table_name']);
        self::assertSame('public', $config['migrations']['table_schema']);
        self::assertFalse($config['migrations']['all_or_nothing']);
        self::assertTrue($config['migrations']['generate_rollback']);
    }

    public function test_migrations_enabled_without_catalog_providers_is_valid_at_config_level() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'connections' => [
                'default' => [
                    'dsn' => 'postgresql://user:pass@localhost:5432/db',
                ],
            ],
            'migrations' => [
                'enabled' => true,
            ],
        ]]);

        self::assertTrue($config['migrations']['enabled']);
    }

    public function test_multiple_connections() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'connections' => [
                'default' => [
                    'dsn' => 'postgresql://user:pass@localhost:5432/db1',
                ],
                'analytics' => [
                    'dsn' => 'postgresql://user:pass@localhost:5432/db2',
                ],
            ],
        ]]);

        self::assertArrayHasKey('default', $config['connections']);
        self::assertArrayHasKey('analytics', $config['connections']);
        self::assertSame('postgresql://user:pass@localhost:5432/db1', $config['connections']['default']['dsn']);
        self::assertSame('postgresql://user:pass@localhost:5432/db2', $config['connections']['analytics']['dsn']);
    }

    public function test_session_default_disabled() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'connections' => [
                'default' => ['dsn' => 'postgresql://user:pass@localhost:5432/db'],
            ],
        ]]);

        self::assertFalse($config['session']['enabled']);
    }

    public function test_session_defaults_when_enabled() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'connections' => [
                'default' => ['dsn' => 'postgresql://user:pass@localhost:5432/db'],
            ],
            'session' => [
                'enabled' => true,
            ],
        ]]);

        $session = $config['session'];
        self::assertTrue($session['enabled']);
        self::assertNull($session['connection']);
        self::assertSame('sessions', $session['table_name']);
        self::assertSame('public', $session['schema']);
        self::assertSame('sess_id', $session['id_col']);
        self::assertSame('sess_data', $session['data_col']);
        self::assertSame('sess_lifetime', $session['lifetime_col']);
        self::assertSame('sess_time', $session['time_col']);
        self::assertSame('transactional', $session['lock_mode']);
        self::assertNull($session['ttl']);
    }

    public function test_session_lock_mode_rejects_invalid_value() : void
    {
        $this->expectException(InvalidConfigurationException::class);

        (new Processor())->processConfiguration(new Configuration(), [[
            'connections' => [
                'default' => ['dsn' => 'postgresql://user:pass@localhost:5432/db'],
            ],
            'session' => [
                'enabled' => true,
                'lock_mode' => 'pessimistic',
            ],
        ]]);
    }

    public function test_session_overrides() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
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
        ]]);

        $session = $config['session'];
        self::assertSame('default', $session['connection']);
        self::assertSame('app_sessions', $session['table_name']);
        self::assertSame('sess', $session['schema']);
        self::assertSame('sid', $session['id_col']);
        self::assertSame('sdata', $session['data_col']);
        self::assertSame('sttl', $session['lifetime_col']);
        self::assertSame('sts', $session['time_col']);
        self::assertSame('advisory', $session['lock_mode']);
        self::assertSame(7200, $session['ttl']);
    }

    public function test_session_ttl_rejects_negative() : void
    {
        $this->expectException(InvalidConfigurationException::class);

        (new Processor())->processConfiguration(new Configuration(), [[
            'connections' => [
                'default' => ['dsn' => 'postgresql://user:pass@localhost:5432/db'],
            ],
            'session' => [
                'enabled' => true,
                'ttl' => -1,
            ],
        ]]);
    }

    public function test_single_connection_with_defaults() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'connections' => [
                'default' => [
                    'dsn' => 'postgresql://user:pass@localhost:5432/db',
                ],
            ],
        ]]);

        self::assertSame('postgresql://user:pass@localhost:5432/db', $config['connections']['default']['dsn']);
        self::assertFalse($config['migrations']['enabled']);
    }

    public function test_telemetry_not_present_by_default() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'connections' => [
                'default' => [
                    'dsn' => 'postgresql://user:pass@localhost:5432/db',
                ],
            ],
        ]]);

        self::assertArrayNotHasKey('telemetry', $config['connections']['default']);
    }

    public function test_telemetry_requires_service_id() : void
    {
        $this->expectException(InvalidConfigurationException::class);

        (new Processor())->processConfiguration(new Configuration(), [[
            'connections' => [
                'default' => [
                    'dsn' => 'postgresql://user:pass@localhost:5432/db',
                    'telemetry' => [],
                ],
            ],
        ]]);
    }

    public function test_telemetry_with_custom_options() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
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
        ]]);

        $telemetry = $config['connections']['default']['telemetry'];
        self::assertSame('my.telemetry', $telemetry['service_id']);
        self::assertSame('my.clock', $telemetry['clock_service_id']);
        self::assertFalse($telemetry['trace_queries']);
        self::assertFalse($telemetry['trace_transactions']);
        self::assertFalse($telemetry['collect_metrics']);
        self::assertTrue($telemetry['log_queries']);
        self::assertSame(500, $telemetry['max_query_length']);
        self::assertTrue($telemetry['include_parameters']);
        self::assertSame(5, $telemetry['max_parameters']);
        self::assertSame(50, $telemetry['max_parameter_length']);
    }

    public function test_telemetry_with_defaults() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'connections' => [
                'default' => [
                    'dsn' => 'postgresql://user:pass@localhost:5432/db',
                    'telemetry' => [
                        'service_id' => 'flow.telemetry',
                    ],
                ],
            ],
        ]]);

        $telemetry = $config['connections']['default']['telemetry'];
        self::assertSame('flow.telemetry', $telemetry['service_id']);
        self::assertNull($telemetry['clock_service_id']);
        self::assertTrue($telemetry['trace_queries']);
        self::assertTrue($telemetry['trace_transactions']);
        self::assertTrue($telemetry['collect_metrics']);
        self::assertFalse($telemetry['log_queries']);
        self::assertSame(1000, $telemetry['max_query_length']);
        self::assertFalse($telemetry['include_parameters']);
        self::assertSame(10, $telemetry['max_parameters']);
        self::assertSame(100, $telemetry['max_parameter_length']);
    }
}
