<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Hash\Algorithm;
use Flow\ETL\Hash\NativePHPHash;
use Flow\ETL\Row\Reference;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;
use Flow\ETL\Transformer\DropDuplicates\Hashes;
use Throwable;

use function serialize;

final readonly class DropDuplicatesTransformer implements Transformer
{
    private Hashes $deduplication;

    /**
     * @var array<Reference|string>
     */
    private array $entries;

    private Algorithm $hashAlgorithm;

    public function __construct(string|Reference ...$entries)
    {
        if ([] === $entries) {
            throw new InvalidArgumentException('DropDuplicatesTransformer requires at least one entry');
        }

        $this->entries = $entries;
        $this->deduplication = new Hashes();
        $this->hashAlgorithm = new NativePHPHash();
    }

    public function transform(Rows $rows, FlowContext $context): Rows
    {
        $context->telemetry()->transformationStarted($this);

        try {
            $newRows = [];

            foreach ($rows as $row) {
                $values = [];

                foreach ($this->entries as $entry) {
                    try {
                        $values[] = $row->get($entry);
                    } catch (InvalidArgumentException) {
                        $values[] = null;
                    }
                }

                $hash = $this->hashAlgorithm->hash(serialize($values));

                if (!$this->deduplication->exists($hash)) {
                    $newRows[] = $row;
                    $this->deduplication->add($hash);
                }
            }

            $result = new Rows($rows->schema(), ...$newRows);

            $context->telemetry()->transformationCompleted($this, [
                TelemetryAttributes::ATTR_TRANSFORMATION_INPUT_ROWS => $rows->count(),
                TelemetryAttributes::ATTR_TRANSFORMATION_OUTPUT_ROWS => $result->count(),
            ]);

            return $result;
        } catch (Throwable $e) {
            $context->telemetry()->transformationFailed($this, $e);

            throw $e;
        }
    }
}
