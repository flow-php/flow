<?php

declare(strict_types=1);

namespace Flow\ETL\Loader;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Loader;
use Flow\ETL\Rows;
use Opis\Closure\SerializableClosure;

/**
 * @implements Loader<array{callback: SerializableClosure}>
 */
final class CallbackLoader implements Loader
{
    private static ?bool $isSerializable = null;

    /**
     * @phpstan-ignore-next-line
     *
     * @param callable(Rows $row) : void $callback
     */
    private $callback;

    public function __construct(callable $callback)
    {
        $this->callback = $callback;
    }

    public function __serialize() : array
    {
        if (!self::isSerializable()) {
            throw new RuntimeException('CallbackLoader is not serializable without "opis/closure" library in your dependencies.');
        }

        return [
            'callback' => new SerializableClosure(\Closure::fromCallable($this->callback)),
        ];
    }

    public function __unserialize(array $data) : void
    {
        if (!self::isSerializable()) {
            throw new RuntimeException('CallbackLoader is not serializable without "opis/closure" library in your dependencies.');
        }

        $this->callback = $data['callback']->getClosure();
    }

    public function load(Rows $rows) : void
    {
        ($this->callback)($rows);
    }

    private static function isSerializable() : bool
    {
        if (self::$isSerializable === null) {
            self::$isSerializable = \class_exists('Opis\Closure\SerializableClosure');
        }

        return self::$isSerializable;
    }
}
