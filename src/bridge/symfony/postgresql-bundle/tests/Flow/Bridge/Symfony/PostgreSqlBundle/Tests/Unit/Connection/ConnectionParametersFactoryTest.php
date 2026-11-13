<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Unit\Connection;

use Flow\Bridge\Symfony\PostgreSqlBundle\Connection\ConnectionParametersFactory;
use Flow\PostgreSql\Client\DsnParser;
use InvalidArgumentException;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use function sprintf;

final class ConnectionParametersFactoryTest extends TestCase
{
    public function test_create_appends_session_settings_after_options_option(): void
    {
        static::assertSame(
            ['options' => '-c search_path=public -c statement_timeout=30s'],
            ConnectionParametersFactory::create(new DsnParser(), 'postgresql://u:p@h/app', [
                'options' => ['options' => '-c search_path=public'],
                'session_settings' => ['statement_timeout' => '30s'],
            ])->options(),
        );
    }

    public function test_create_applies_dbname_suffix_after_overrides(): void
    {
        static::assertSame(
            'other_test',
            ConnectionParametersFactory::create(new DsnParser(), 'postgresql://u:p@h/app', [
                'dbname' => 'other',
                'dbname_suffix' => '_test',
                'options' => ['connect_timeout' => 5],
            ])->database(),
        );
    }

    public function test_create_applies_options_over_dsn_and_keeps_other_dsn_options(): void
    {
        static::assertSame(
            ['connect_timeout' => '5', 'sslmode' => 'require'],
            ConnectionParametersFactory::create(
                new DsnParser(),
                'postgresql://u:p@h/app?connect_timeout=10&sslmode=require',
                ['options' => ['connect_timeout' => 5]],
            )->options(),
        );
    }

    public function test_create_casts_integer_option_to_string(): void
    {
        static::assertSame(
            ['tcp_user_timeout' => '30000'],
            ConnectionParametersFactory::create(new DsnParser(), 'postgresql://u:p@h/app', [
                'options' => ['tcp_user_timeout' => 30000],
            ])->options(),
        );
    }

    public function test_create_from_dsn_only(): void
    {
        static::assertSame(
            'host=localhost port=5432 dbname=app user=u password=p',
            ConnectionParametersFactory::create(new DsnParser(), 'postgresql://u:p@localhost:5432/app')->toString(),
        );
    }

    public function test_create_keeps_dsn_parts_when_overrides_are_null(): void
    {
        static::assertSame(
            'host=h port=5433 dbname=app user=u password=p',
            ConnectionParametersFactory::create(new DsnParser(), 'postgresql://u:p@h:5433/app', [
                'dbname' => null,
                'host' => null,
                'port' => null,
                'user' => null,
                'password' => null,
            ])->toString(),
        );
    }

    public function test_create_overrides_replace_dsn_parts(): void
    {
        $params = ConnectionParametersFactory::create(new DsnParser(), 'postgresql://u:p@h:5433/app', [
            'host' => 'db.internal',
            'port' => 5544,
            'user' => 'svc',
            'password' => 'pw',
        ]);

        static::assertSame('db.internal', $params->host());
        static::assertSame(5544, $params->port());
        static::assertSame('svc', $params->user());
        static::assertSame('pw', $params->password());
    }

    #[TestWith([1.5, 'float'])]
    #[TestWith([true, 'bool'])]
    #[TestWith([null, 'null'])]
    public function test_create_rejects_option_value_that_is_not_string_or_integer(
        bool|float|null $value,
        string $type,
    ): void {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(sprintf(
            'Connection option "keepalives_idle" must be a string or an integer, %s given.',
            $type,
        ));

        ConnectionParametersFactory::create(new DsnParser(), 'postgresql://u:p@h/app', [
            'options' => ['keepalives_idle' => $value],
        ]);
    }

    public function test_create_rejects_session_setting_value_that_is_not_string_or_integer(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Session setting "statement_timeout" must be a string or an integer, bool given.',
        );

        ConnectionParametersFactory::create(new DsnParser(), 'postgresql://u:p@h/app', [
            'session_settings' => ['statement_timeout' => false],
        ]);
    }

    public function test_create_writes_session_settings_into_options(): void
    {
        static::assertSame(
            ['options' => '-c statement_timeout=30s -c lock_timeout=10000'],
            ConnectionParametersFactory::create(new DsnParser(), 'postgresql://u:p@h/app', [
                'session_settings' => ['statement_timeout' => '30s', 'lock_timeout' => 10000],
            ])->options(),
        );
    }
}
