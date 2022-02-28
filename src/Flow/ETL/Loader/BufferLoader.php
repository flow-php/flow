<?php

declare(strict_types=1);

namespace Flow\ETL\Loader;

use Flow\ETL\Loader;
use Flow\ETL\Pipeline\Closure;
use Flow\ETL\Rows;

final class BufferLoader implements Closure, Loader
{
    private Rows $buffer;

    private int $bufferSize;

    private Loader $overflowLoader;

    public function __construct(Loader $overflowLoader, int $bufferSize)
    {
        $this->overflowLoader = $overflowLoader;
        $this->bufferSize = $bufferSize;
        $this->buffer = new Rows();
    }

    /**
     * @return array{overflow_loader: Loader, buffer_size: int}
     */
    public function __serialize() : array
    {
        return [
            'overflow_loader' => $this->overflowLoader,
            'buffer_size' => $this->bufferSize,
        ];
    }

    /**
     * @param array{overflow_loader: Loader, buffer_size: int} $data
     * @psalm-suppress MoreSpecificImplementedParamType
     */
    public function __unserialize(array $data) : void
    {
        $this->buffer = new Rows();
        $this->overflowLoader = $data['overflow_loader'];
        $this->bufferSize = $data['buffer_size'];
    }

    public function closure(Rows $rows) : void
    {
        if ($this->buffer->count()) {
            $this->overflowLoader->load($rows);
        }
    }

    public function load(Rows $rows) : void
    {
        if ($this->buffer->count() < $this->bufferSize) {
            $this->buffer = $this->buffer->merge($rows);
        }

        if ($this->buffer->count() > $this->bufferSize) {
            foreach ($this->buffer->chunks($this->bufferSize) as $bufferChunk) {
                if ($bufferChunk->count() === $this->bufferSize) {
                    $this->overflowLoader->load($bufferChunk);
                } else {
                    $this->buffer = $bufferChunk;
                }
            }
        }

        if ($this->buffer->count() === $this->bufferSize) {
            $this->overflowLoader->load($this->buffer);

            $this->buffer = new Rows();
        }
    }
}
