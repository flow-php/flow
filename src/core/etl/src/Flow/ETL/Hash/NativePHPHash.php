<?php

declare(strict_types=1);

namespace Flow\ETL\Hash;

use InvalidArgumentException;

use function hash;
use function hash_algos;
use function in_array;
use function sprintf;

final class NativePHPHash implements Algorithm
{
    private static ?self $instance = null;
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

    public static function xxh128(string $string): string
    {
        self::$instance ??= new self('xxh128');

        return self::$instance->hash($string);
    }

    public function hash(string $value): string
    {
        return hash($this->algorithm, $value, $this->binary, $this->options);
    }
}
