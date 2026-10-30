<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\IO\Inference\SchemaInference;
use Flow\ETL\IO\Inference\SchemaInferrer;
use Flow\ETL\IO\Inference\Sniffer;
use Flow\ETL\Tests\Double\PresniffedSniffer;
use Flow\ETL\Values\Schema;

use function array_values;
use function ksort;

final class ReversedSniffing
{
    /**
     * Sniffs every unit whole, last unit first, then lets infer() merge the partials in unit order - the schema a
     * run whose units finished in the opposite order would infer. Equal to infer() over $sniffer when the result
     * does not depend on execution order.
     */
    public static function schema(Sniffer $sniffer, SchemaInference $inference): Schema
    {
        $samples = [];

        for ($unit = $sniffer->units() - 1; $unit >= 0; $unit--) {
            $samples[$unit] = $sniffer->sniff($unit, -1);
        }

        ksort($samples);

        return (new SchemaInferrer($inference))->infer(
            new PresniffedSniffer($sniffer->names(), array_values($samples)),
        )->schema;
    }
}
