<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Serializer\Closure;
use Flow\ETL\Transformer;
use Opis\Closure\SerializableClosure;

/**
 * @psalm-immutable
 */
final class CallbackRowTransformer implements Transformer
{
    /**
     * @psalm-var pure-callable(Row) : Row
     * @phpstan-var callable(Row) : Row
     */
    private $callable;

    /**
     * @psalm-param pure-callable(Row) : Row $callable
     *
     * @param callable(Row) : Row $callable
     */
    public function __construct(callable $callable)
    {
        $this->callable = $callable;
    }

    /**
     * @return array{callable: SerializableClosure}
     */
    public function __serialize() : array
    {
        /** @psalm-suppress ImpureMethodCall */
        if (!Closure::isSerializable()) {
            throw new RuntimeException('CallbackRowTransformer is not serializable without "opis/closure" library in your dependencies.');
        }

        return [
            'callable' => new SerializableClosure(\Closure::fromCallable($this->callable)),
        ];
    }

    /**
     * @param array{callable: SerializableClosure} $data
     * @psalm-suppress MoreSpecificImplementedParamType
     */
    public function __unserialize(array $data) : void
    {
        /** @psalm-suppress ImpureMethodCall */
        if (!Closure::isSerializable()) {
            throw new RuntimeException('CallbackRowTransformer is not serializable without "opis/closure" library in your dependencies.');
        }

        $this->callable = $data['callable']->getClosure();
    }

    public function transform(Rows $rows) : Rows
    {
        return $rows->map($this->callable);
    }
}
