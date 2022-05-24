<?php

declare(strict_types=1);

namespace Flow\ETL\Stream;

use Flow\Serializer\Serializable;

/**
 * @template T
 * @extends Serializable<T>
 */
interface FileStream extends Serializable
{
    /**
     * @return array<string, mixed>
     */
    public function options() : array;

    public function scheme() : string;

    public function uri() : string;
}
