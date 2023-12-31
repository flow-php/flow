<?php

declare(strict_types=1);

namespace Flow\ETL\Loader;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Rows;
use Flow\ETL\Serializer\Closure;
use Laravel\SerializableClosure\SerializableClosure;

final class CallbackLoader implements Loader
{
    /**
     * @phpstan-ignore-next-line
     *
     * @param callable(Rows $row, FlowContext $context) : void $callback
     */
    private $callback;

    public function __construct(callable $callback)
    {
        $this->callback = $callback;
    }

    public function __serialize() : array
    {
        if (!Closure::isSerializable()) {
            throw new RuntimeException('CallbackLoader is not serializable without "opis/closure" library in your dependencies.');
        }

        return [
            'callback' => new SerializableClosure(\Closure::fromCallable($this->callback)),
        ];
    }

    public function __unserialize(array $data) : void
    {
        if (!Closure::isSerializable()) {
            throw new RuntimeException('CallbackLoader is not serializable without "opis/closure" library in your dependencies.');
        }

        $this->callback = $data['callback']->getClosure();
    }

    public function load(Rows $rows, FlowContext $context) : void
    {
        ($this->callback)($rows, $context);
    }
}
