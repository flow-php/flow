<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\ETL\Row\Encoder;
use Flow\ETL\Schema;

/**
 * @implements Encoder<string>
 */
final class AdaptiveFloeEncoder implements Encoder
{
    /**
     * @var Encoder<string>
     */
    private readonly Encoder $delegate;

    public function __construct(Schema $schema)
    {
        $this->delegate = NativeFloeEncoder::isSupported()
            ? new NativeFloeEncoder($schema)
            : new PhpFloeEncoder($schema);
    }

    public function decode(array $batch): array
    {
        return $this->delegate->decode($batch);
    }

    public function encode(array $batch): array
    {
        return $this->delegate->encode($batch);
    }
}
