<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\RowMapper;

use Flow\PostgreSql\Client\{Client, Context as ClientContext, Query};
use Flow\PostgreSql\Client\Exception\ContextException;
use Flow\PostgreSql\Schema\Catalog;
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type;

final readonly class Context
{
    public function __construct(
        private Query $query,
        private Client $client,
        private ClientContext $clientContext,
    ) {
    }

    /**
     * @return array<string, mixed>
     */
    public function all() : array
    {
        return $this->clientContext->all();
    }

    public function catalog() : ?Catalog
    {
        return $this->clientContext->catalog();
    }

    public function client() : Client
    {
        return $this->client;
    }

    public function clientContext() : ClientContext
    {
        return $this->clientContext;
    }

    /**
     * @template T
     *
     * @param Type<T> $type
     *
     * @throws ContextException when the key is missing
     * @throws InvalidTypeException when the stored value does not satisfy $type
     *
     * @return T
     */
    public function get(string $key, Type $type) : mixed
    {
        return $this->clientContext->get($key, $type);
    }

    public function has(string $key) : bool
    {
        return $this->clientContext->has($key);
    }

    public function merge(self $other) : self
    {
        return new self(
            query: $other->query,
            client: $other->client,
            clientContext: $this->clientContext->merge($other->clientContext),
        );
    }

    public function query() : Query
    {
        return $this->query;
    }

    public function with(string $key, mixed $value) : self
    {
        return new self(
            query: $this->query,
            client: $this->client,
            clientContext: $this->clientContext->with($key, $value),
        );
    }
}
