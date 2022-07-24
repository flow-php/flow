<?php

declare(strict_types=1);

namespace Flow\ETL\Async\Socket\Communication;

use Flow\Serializer\Serializer;

final class MessageBuffer
{
    /**
     * @var \WeakMap<object, array<string>>
     */
    private \WeakMap $buffers;

    private Serializer $serializer;

    public function __construct(Serializer $serializer)
    {
        /**
         * @phpstan-ignore-next-line
         *
         * @var \WeakMap<object, array<string>>
         */
        $this->buffers = new \WeakMap();
        $this->serializer = $serializer;
    }

    public function buffer(object $connection, string $data) : ?Message
    {
        if (!$this->buffers->offsetExists($connection)) {
            $this->buffers->offsetSet($connection, []);
        }

        if (\str_starts_with($data, '|') && !\str_ends_with($data, '|')) {
            /** @phpstan-ignore-next-line  */
            $this->buffers->offsetSet($connection, \array_merge($this->buffers->offsetGet($connection), [$data]));

            return null;
        }

        if (!\str_starts_with($data, '|') && !\str_ends_with($data, '|')) {
            /** @phpstan-ignore-next-line  */
            $this->buffers->offsetSet($connection, \array_merge($this->buffers->offsetGet($connection), [$data]));

            return null;
        }

        if (!\str_starts_with($data, '|') && \str_ends_with($data, '|')) {
            /** @phpstan-ignore-next-line  */
            $this->buffers->offsetSet($connection, \array_merge($this->buffers->offsetGet($connection), [$data]));

            /** @phpstan-ignore-next-line  */
            $data = \implode($this->buffers->offsetGet($connection));
            $this->buffers->offsetSet($connection, []);
        }

        /** @var Message $message */
        $message = $this->serializer->unserialize(\trim($data, '|'));

        return $message;
    }
}
