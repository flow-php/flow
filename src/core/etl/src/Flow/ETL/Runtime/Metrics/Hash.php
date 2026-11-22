<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime\Metrics;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Values\Column\MultisetDigest;

use function array_key_exists;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function sprintf;

final readonly class Hash
{
    public function __construct(
        public int $rows,
        public string $value,
    ) {}

    public static function empty(): self
    {
        return new self(0, MultisetDigest::empty()->hex());
    }

    /**
     * @param array<array-key, mixed> $data
     *
     * @throws InvalidArgumentException
     */
    public static function fromArray(array $data): self
    {
        foreach (['rows' => type_integer(), 'value' => type_string()] as $key => $type) {
            if (!array_key_exists($key, $data) || !$type->isValid($data[$key])) {
                throw new InvalidArgumentException(sprintf('A hash needs "%s" of type %s', $key, $type->toString()));
            }
        }

        /** @var array{rows: int, value: string} $data */
        return new self($data['rows'], $data['value']);
    }

    /**
     * @return array{rows: int, value: string}
     */
    public function normalize(): array
    {
        return ['rows' => $this->rows, 'value' => $this->value];
    }

    public function plus(self $other): self
    {
        return new self(
            $this->rows + $other->rows,
            MultisetDigest::fromHex($this->value)->plus(MultisetDigest::fromHex($other->value))->hex(),
        );
    }
}
