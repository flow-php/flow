<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Unit\DependencyInjection;

use Flow\Bridge\Symfony\PostgreSqlBundle\FlowPostgreSqlBundle;
use Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Context\ExtensionContext;
use Flow\PostgreSql\Client\Client;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(FlowPostgreSqlBundle::class)]
final class ConnectionLazyTest extends TestCase
{
    private ExtensionContext $context;

    protected function setUp(): void
    {
        $this->context = new ExtensionContext();
    }

    public function test_client_is_lazy_proxied_against_client_interface_by_default(): void
    {
        $container = $this->context->load([
            'connections' => [
                'default' => ['dsn' => 'postgresql://user:pass@localhost:5432/db'],
            ],
        ]);

        $clientDef = $container->getDefinition('flow.postgresql.default.client');

        static::assertTrue($clientDef->isLazy());
        static::assertSame([['interface' => Client::class]], $clientDef->getTag('proxy'));
    }

    public function test_client_is_not_lazy_when_disabled(): void
    {
        $container = $this->context->load([
            'connections' => [
                'default' => [
                    'dsn' => 'postgresql://user:pass@localhost:5432/db',
                    'lazy' => false,
                ],
            ],
        ]);

        $clientDef = $container->getDefinition('flow.postgresql.default.client');

        static::assertFalse($clientDef->isLazy());
        static::assertFalse($clientDef->hasTag('proxy'));
    }

    public function test_test_transaction_rollback_client_is_still_lazy_by_default(): void
    {
        $container = $this->context->load([
            'connections' => [
                'default' => [
                    'dsn' => 'postgresql://user:pass@localhost:5432/db',
                    'test_transaction_rollback' => true,
                ],
            ],
        ]);

        $clientDef = $container->getDefinition('flow.postgresql.default.client');

        static::assertTrue($clientDef->isLazy());
        static::assertSame([['interface' => Client::class]], $clientDef->getTag('proxy'));
    }
}
