<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client;

use Flow\PostgreSql\Client\Exception\ContextException;
use Flow\PostgreSql\Schema\Catalog;
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type;

final readonly class Context
{
    /**
     * @param array<string, mixed> $data
     */
    public function __construct(
        private ?Catalog $catalog = null,
        private array $data = [],
    ) {}

    /**
     * @return array<string, mixed>
     */
    public function all(): array
    {
        return $this->data;
    }

    public function catalog(): ?Catalog
    {
        return $this->catalog;
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
    public function get(string $key, Type $type): mixed
    {
        if (!\array_key_exists($key, $this->data)) {
            throw ContextException::keyNotFound($key);
        }

        return $type->assert($this->data[$key]);
    }

    public function has(string $key): bool
    {
        return \array_key_exists($key, $this->data);
    }

    public function merge(self $other): self
    {
        return new self(catalog: $other->catalog ?? $this->catalog, data: \array_replace($this->data, $other->data));
    }

    public function with(string $key, mixed $value): self
    {
        $data = $this->data;
        $data[$key] = $value;

        return new self(catalog: $this->catalog, data: $data);
    }
}
