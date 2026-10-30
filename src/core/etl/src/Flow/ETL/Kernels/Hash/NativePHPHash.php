<?php

declare(strict_types=1);

namespace Flow\ETL\Kernels\Hash;

use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use InvalidArgumentException;
use SensitiveParameter;

use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_string;
use function hash;
use function hash_algos;
use function in_array;
use function sprintf;

final class NativePHPHash implements Algorithm, Describable
{
    private static ?array $algorithms = null;

    /**
     * @param array<array-key, mixed> $options
     */
    public function __construct(
        private readonly string $algorithm = 'xxh128',
        private readonly bool $binary = false,
        private readonly array $options = [],
    ) {
        self::$algorithms ??= hash_algos();

        if (!in_array($algorithm, self::$algorithms, true)) {
            throw new InvalidArgumentException(sprintf('Hashing algorithm "%s" is not supported', $algorithm));
        }
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            type_string()->assert($data['algorithm'] ?? null),
            type_boolean()->assert($data['binary'] ?? null),
            type_array()->assert($data['options'] ?? null),
        );
    }

    public function describe(): ?Description
    {
        return new Description(self::class, [
            'algorithm' => $this->algorithm,
            'binary' => $this->binary,
            'options' => $this->options,
        ]);
    }

    public function hash(string $value): string
    {
        return hash($this->algorithm, $value, $this->binary, $this->options);
    }
}
