<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Serializer\Closure;
use Flow\ETL\Transformer;
use Laravel\SerializableClosure\SerializableClosure;

/**
 * @implements Transformer<array{callable: SerializableClosure}>
 */
final class CallbackRowTransformer implements Transformer
{
    /**
     * @phpstan-var callable(Row) : Row
     */
    private $callable;

    /**
     * @param callable(Row) : Row $callable
     */
    public function __construct(callable $callable)
    {
        $this->callable = $callable;
    }

    public function __serialize() : array
    {
        if (!Closure::isSerializable()) {
            throw new RuntimeException('CallbackRowTransformer is not serializable without "opis/closure" library in your dependencies.');
        }

        return [
            'callable' => new SerializableClosure(\Closure::fromCallable($this->callable)),
        ];
    }

    public function __unserialize(array $data) : void
    {
        if (!Closure::isSerializable()) {
            throw new RuntimeException('CallbackRowTransformer is not serializable without "opis/closure" library in your dependencies.');
        }

        $this->callable = $data['callable']->getClosure();
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        return $rows->map($this->callable);
    }
}
