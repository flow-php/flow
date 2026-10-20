<?php

declare(strict_types=1);

namespace Flow\Parquet\BloomFilter;

use function chr;
use function count_chars;
use function decbin;
use function intdiv;
use function log;
use function min;
use function ord;
use function str_repeat;
use function strlen;
use function substr;
use function substr_count;

/**
 * Sized and folded like arrow-rs 60: optimal_num_of_bytes, then fold_to_target_fpp.
 */
final class SbbfBuilder
{
    private const int MAX_BYTES = 128 * 1024 * 1024;

    private readonly int $blocks;

    private function __construct(
        private string $bitset,
        private readonly float $fpp,
    ) {
        $this->blocks = intdiv(strlen($bitset), Sbbf::BLOCK_BYTES);
    }

    public static function for(BloomFilterOptions $options): self
    {
        $bits = min(
            (-8.0 * ($options->ndv ?? BloomFilterOptions::DEFAULT_NDV)) / log(1.0 - $options->fpp ** (1.0 / 8.0)),
            self::MAX_BYTES * 8.0,
        );
        $bytes = Sbbf::BLOCK_BYTES;

        while ($bytes < intdiv((int) $bits, 8)) {
            $bytes <<= 1;
        }

        return new self(str_repeat("\0", $bytes), $options->fpp);
    }

    public function build(): Sbbf
    {
        $folds = $this->folds();

        if ($folds === 0) {
            return Sbbf::fromBytes($this->bitset);
        }

        $groupBytes = Sbbf::BLOCK_BYTES << $folds;
        $folded = '';

        for ($start = 0; $start < strlen($this->bitset); $start += $groupBytes) {
            $block = substr($this->bitset, $start, Sbbf::BLOCK_BYTES);

            for ($next = Sbbf::BLOCK_BYTES; $next < $groupBytes; $next += Sbbf::BLOCK_BYTES) {
                $block |= substr($this->bitset, $start + $next, Sbbf::BLOCK_BYTES);
            }

            $folded .= $block;
        }

        return Sbbf::fromBytes($folded);
    }

    public function insert(int $hash): void
    {
        foreach (Sbbf::bits($hash, $this->blocks) as $bit) {
            $byte = $bit >> 3;
            $this->bitset[$byte] = chr(ord($this->bitset[$byte]) | (1 << ($bit & 7)));
        }
    }

    /**
     * arrow-rs num_folds_for_target_fpp: the most halvings whose estimated false positive probability, with every
     * block filled at the average rate f, stays within fpp: (1 - (1 - f)^(2^k))^8.
     */
    private function folds(): int
    {
        $maxFolds = strlen(decbin($this->blocks)) - 1;
        $setBits = 0;

        foreach (count_chars($this->bitset, 1) as $byte => $count) {
            $setBits += $count * substr_count(decbin($byte), '1');
        }

        if ($setBits === 0) {
            return $maxFolds;
        }

        $empty = 1.0 - ($setBits / ($this->blocks * 256.0));
        $folds = 0;

        while ($folds < $maxFolds) {
            $empty *= $empty;
            $filled = 1.0 - $empty;
            $filled *= $filled;
            $filled *= $filled;

            if (($filled * $filled) > $this->fpp) {
                break;
            }

            $folds++;
        }

        return $folds;
    }
}
