<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\RowMapper;

use function Flow\Types\DSL\{type_integer, type_string};
use Flow\PostgreSql\Client\{Context as ClientContext, Query};
use Flow\PostgreSql\Client\RowMapper\Context;
use Flow\PostgreSql\Schema\Catalog;
use Flow\PostgreSql\Tests\Mother\{MapperContextMother, StubClient};
use PHPUnit\Framework\TestCase;

final class ContextTest extends TestCase
{
    public function test_all_proxies_to_client_context() : void
    {
        self::assertSame(['tenant' => 42], MapperContextMother::withData(['tenant' => 42])->all());
    }

    public function test_catalog_proxies_to_client_context() : void
    {
        $catalog = new Catalog([]);

        self::assertSame(
            $catalog,
            MapperContextMother::with(clientContext: new ClientContext(catalog: $catalog))->catalog(),
        );
    }

    public function test_client_context_returns_provided_instance() : void
    {
        $clientContext = new ClientContext(data: ['a' => 1]);

        self::assertSame(
            $clientContext,
            MapperContextMother::with(clientContext: $clientContext)->clientContext(),
        );
    }

    public function test_client_returns_provided_instance() : void
    {
        $client = new StubClient();

        self::assertSame($client, MapperContextMother::with(client: $client)->client());
    }

    public function test_get_proxies_to_client_context() : void
    {
        self::assertSame(42, MapperContextMother::withData(['tenant' => 42])->get('tenant', type_integer()));
    }

    public function test_has_proxies_to_client_context() : void
    {
        $context = MapperContextMother::withData(['tenant' => 42]);

        self::assertTrue($context->has('tenant'));
        self::assertFalse($context->has('missing'));
    }

    public function test_merge_combines_client_contexts_and_takes_other_query_and_client() : void
    {
        $selfClient = new StubClient();
        $otherClient = new StubClient();

        $self = new Context(
            new Query('SELECT 1'),
            $selfClient,
            new ClientContext(data: ['a' => 1, 'b' => 2]),
        );
        $other = new Context(
            new Query('SELECT 2'),
            $otherClient,
            new ClientContext(data: ['b' => 20, 'c' => 30]),
        );

        $merged = $self->merge($other);

        self::assertSame('SELECT 2', $merged->query()->sql());
        self::assertSame($otherClient, $merged->client());
        self::assertSame(['a' => 1, 'b' => 20, 'c' => 30], $merged->all());
    }

    public function test_query_returns_provided_instance() : void
    {
        $query = new Query('SELECT 1', [1]);

        self::assertSame($query, MapperContextMother::with(query: $query)->query());
    }

    public function test_with_preserves_query_and_client() : void
    {
        $client = new StubClient();
        $query = new Query('SELECT 1');

        $modified = MapperContextMother::with(query: $query, client: $client)->with('key', 'value');

        self::assertSame($query, $modified->query());
        self::assertSame($client, $modified->client());
        self::assertSame('value', $modified->get('key', type_string()));
    }

    public function test_with_returns_new_instance() : void
    {
        $context = MapperContextMother::any();
        $modified = $context->with('key', 'value');

        self::assertNotSame($context, $modified);
        self::assertFalse($context->has('key'));
        self::assertTrue($modified->has('key'));
    }
}
