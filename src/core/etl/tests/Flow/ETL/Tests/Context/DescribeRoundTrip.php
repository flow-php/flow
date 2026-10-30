<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Describe\Json;
use Flow\ETL\Foundation\Describe\Rebuild;
use RuntimeException;

use function Flow\Types\DSL\type_instance_of;

final class DescribeRoundTrip
{
    /**
     * $describable after describe(), a JSON round trip and Rebuild::of(), the way a worker receives it.
     *
     * @template T of Describable
     *
     * @param T $describable
     *
     * @return T
     */
    public static function rebuilt(Describable $describable): Describable
    {
        return type_instance_of($describable::class)->assert((new Rebuild())->of(
            Json::decode(
                Json::encode(
                    ($describable->describe() ?? throw new RuntimeException($describable::class
                    . ' does not describe'))->normalize(),
                ),
            ),
            $describable::class,
        ));
    }
}
