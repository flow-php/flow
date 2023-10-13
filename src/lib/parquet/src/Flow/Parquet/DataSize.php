<?php declare(strict_types=1);

namespace Flow\Parquet;

final class DataSize
{
    private ?int $bytes = null;

    public function __construct(private int $bits)
    {
    }

    public function add(int|self $bits) : void
    {
        if ($bits instanceof self) {
            $this->bits += $bits->bits;
            $this->bytes = (int) \round($this->bits / 8, 0, PHP_ROUND_HALF_DOWN);

            return;
        }

        $this->bits += $bits;
        $this->bytes = (int) \round($this->bits / 8, 0, PHP_ROUND_HALF_DOWN);
    }

    public function bits() : int
    {
        return $this->bits;
    }

    public function bytes() : int
    {
        if ($this->bytes === null) {
            $this->bytes = (int) \round($this->bits / 8, 0, PHP_ROUND_HALF_DOWN);
        }

        return $this->bytes;
    }

    public function sub(int|self $bits) : void
    {
        if ($bits instanceof self) {
            $this->bits -= $bits->bits;
            $this->bytes = (int) \round($this->bits / 8, 0, PHP_ROUND_HALF_DOWN);

            return;
        }

        $this->bits -= $bits;
        $this->bytes = (int) \round($this->bits / 8, 0, PHP_ROUND_HALF_DOWN);
    }
}
