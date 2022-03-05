<?php declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use Flow\Serializer\Serializable;

/**
 * @template T
 * @extends Serializable<T>
 *
 * @internal
 */
interface Pipe extends Serializable
{
}
