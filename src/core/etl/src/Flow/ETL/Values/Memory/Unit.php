<?php

declare(strict_types=1);

namespace Flow\ETL\Values\Memory;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Exception\RuntimeException;

use function abs;
use function getrusage;
use function preg_match;
use function round;
use function str_replace;
use function strtoupper;

final readonly class Unit
{
    private function __construct(
        private int $bytes,
    ) {}

    public static function fromBytes(int $bytes): self
    {
        return new self($bytes);
    }

    public static function fromGb(int $gb): self
    {
        return new self($gb * 1024 * 1024 * 1024);
    }

    public static function fromKb(int $kb): self
    {
        return new self($kb * 1024);
    }

    public static function fromMb(int $mb): self
    {
        return new self($mb * 1024 * 1024);
    }

    /**
     * @throws InvalidArgumentException
     */
    public static function fromString(string $memoryString): self
    {
        $limit = str_replace(' ', '', $memoryString);
        $matches = [];

        if (preg_match('/^(\d+)(B|KB?|MB?|GB?)?$/i', $limit, $matches) !== 1) {
            throw new InvalidArgumentException("Can't extract memory limit in bytes from php ini value: {$limit}");
        }

        return match (strtoupper($matches[2] ?? 'B')[0]) {
            'K' => self::fromKb((int) $matches[1]),
            'M' => self::fromMb((int) $matches[1]),
            'G' => self::fromGb((int) $matches[1]),
            default => self::fromBytes((int) $matches[1]),
        };
    }

    /**
     * The peak resident set size of this process so far, as the OS counts it: Rust allocations and shared libraries
     * included. getrusage() reports it in bytes on macOS and in KiB elsewhere.
     */
    public static function processPeak(): self
    {
        $maxRss = (getrusage() ?: throw new RuntimeException('getrusage() failed'))['ru_maxrss'];

        return new self(PHP_OS_FAMILY === 'Darwin' ? $maxRss : $maxRss * 1024);
    }

    public function absolute(): self
    {
        return new self(abs($this->bytes));
    }

    public function diff(self $unit): self
    {
        return new self($this->bytes - $unit->bytes);
    }

    public function inBytes(): int
    {
        return $this->bytes;
    }

    public function inGb(int $precision = 2): float
    {
        return round($this->bytes / (1024 * 1024 * 1024), $precision);
    }

    public function inKb(int $precision = 2): float
    {
        return round($this->bytes / 1024, $precision);
    }

    public function inMb(int $precision = 2): float
    {
        return round($this->bytes / (1024 * 1024), $precision);
    }

    public function isGreaterThan(self $unit): bool
    {
        return $this->bytes > $unit->bytes;
    }

    public function isLowerThan(self $unit): bool
    {
        return $this->bytes < $unit->bytes;
    }

    public function percentage(int $value): self
    {
        return new self((int) round(($value / 100) * $this->bytes));
    }
}
