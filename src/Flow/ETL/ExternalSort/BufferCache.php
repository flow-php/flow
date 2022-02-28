<?php

declare(strict_types=1);

namespace Flow\ETL\ExternalSort;

use Flow\ETL\Cache;
use Flow\ETL\Rows;

/**
 * @internal
 */
final class BufferCache
{
    /**
     * @var array<string, Rows>
     */
    private array $buffers;

    private int $bufferSize;

    private Cache $overflowCache;

    public function __construct(Cache $overflowCache, int $bufferSize)
    {
        $this->overflowCache = $overflowCache;
        $this->bufferSize = $bufferSize;
        $this->buffers = [];
    }

    public function add(string $id, Rows $rows) : void
    {
        if (!\array_key_exists($id, $this->buffers)) {
            $this->buffers[$id] = new Rows();
        }

        if ($this->buffers[$id]->count() < $this->bufferSize) {
            $this->buffers[$id] = $this->buffers[$id]->merge($rows);
        }

        if ($this->buffers[$id]->count() > $this->bufferSize) {
            foreach ($this->buffers[$id]->chunks($this->bufferSize) as $bufferChunk) {
                if ($bufferChunk->count() === $this->bufferSize) {
                    $this->overflowCache->add($id, $bufferChunk);
                } else {
                    $this->buffers[$id] = $bufferChunk;
                }
            }
        }

        if ($this->buffers[$id]->count() == $this->bufferSize) {
            $this->overflowCache->add($id, $this->buffers[$id]);
            $this->buffers[$id] = new Rows();
        }
    }

    public function close() : void
    {
        foreach ($this->buffers as $id => $buffer) {
            if ($buffer->count()) {
                $this->overflowCache->add($id, $buffer);
            }
        }
    }
}
