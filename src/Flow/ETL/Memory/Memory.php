<?php

declare(strict_types=1);

namespace Flow\ETL\Memory;

use Flow\Serializer\Serializable;

interface Memory extends Serializable
{
    /**
     * @param array<array<string, mixed>> $data
     */
    public function save(array $data) : void;

    /**
     * @psalm-mutation-free
     *
     * @return array<array<string, mixed>>
     */
    public function dump() : array;

    /**
     * @param callable(array<string, mixed>) : mixed $callback
     *
     * @return array<mixed>
     */
    public function map(callable $callback) : array;

    /**
     * @param int $size
     *
     * @return array<self>
     */
    public function chunks(int $size) : array;

    /**
     * This method is a combination of array_map and array_values functions.
     *
     * Turns: [['id' => 1], ['id' => 2], ['id' => 3], ['id' => 4]]
     * Into: [1, 2, 3, 4]
     *
     * @return array<mixed>
     */
    public function flatValues() : array;

    public function count() : int;
}
