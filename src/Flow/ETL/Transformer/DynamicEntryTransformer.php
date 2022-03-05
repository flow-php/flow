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
 * @implements Transformer<array{generator: SerializableClosure}>
 * @psalm-immutable
 */
final class DynamicEntryTransformer implements Transformer
{
    /**
     * @var callable(Row) : Row\Entries
     */
    private $generator;

    /**
     * @param callable(Row) : Row\Entries $generator
     */
    public function __construct(callable $generator)
    {
        $this->generator = $generator;
    }

    public function __serialize() : array
    {
        if (!Closure::isSerializable()) {
            throw new RuntimeException('DynamicEntryTransformer is not serializable without "opis/closure" library in your dependencies.');
        }

        return [
            'generator' => new SerializableClosure(\Closure::fromCallable($this->generator)),
        ];
    }

    public function __unserialize(array $data) : void
    {
        if (!Closure::isSerializable()) {
            throw new RuntimeException('DynamicEntryTransformer is not serializable without "opis/closure" library in your dependencies.');
        }

        $this->generator = $data['generator']->getClosure();
    }

    public function transform(Rows $rows) : Rows
    {
        /**
         * @psalm-var pure-callable(Row) : Row $transformer
         */
        $transformer = function (Row $row) : Row {
            return new Row($row->entries()->merge(($this->generator)($row)));
        };

        return $rows->map($transformer);
    }
}
