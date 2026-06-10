<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Seal;

use CmsIg\Seal\EngineInterface;
use Flow\ETL\Adapter\Seal\RowsNormalizer\EntryNormalizer;
use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Rows;
use Generator;
use Throwable;

use function is_int;
use function is_string;

final class SealLoader implements Loader
{
    private int $bulkSize = 100;

    private string $identifierEntry = 'id';

    public function __construct(
        private readonly EngineInterface $engine,
        private readonly string $index,
        private readonly Operation $operation = Operation::UPSERT,
    ) {}

    public function load(Rows $rows, FlowContext $context): void
    {
        if (!$rows->count()) {
            return;
        }

        $context->telemetry()->loadingStarted($this);

        try {
            $this->engine->bulk(
                $this->index,
                $this->operation === Operation::UPSERT
                    ? (new RowsNormalizer(new EntryNormalizer()))->normalize($rows)
                    : [],
                $this->operation === Operation::DELETE ? $this->deleteIdentifiers($rows) : [],
                $this->bulkSize,
            );

            $context->telemetry()->loadingCompleted($this, [TelemetryAttributes::ATTR_LOADING_ROWS => $rows->count()]);
        } catch (Throwable $e) {
            $context->telemetry()->loadingFailed($this, $e);

            throw $e;
        }
    }

    public function withBulkSize(int $bulkSize): self
    {
        $this->bulkSize = $bulkSize;

        return $this;
    }

    public function withIdentifierEntry(string $entry): self
    {
        $this->identifierEntry = $entry;

        return $this;
    }

    /**
     * @return Generator<int, string>
     */
    private function deleteIdentifiers(Rows $rows): Generator
    {
        $normalizer = new EntryNormalizer();

        foreach ($rows as $row) {
            $identifier = $normalizer->normalize($row->get($this->identifierEntry));

            if (!is_string($identifier) && !is_int($identifier)) {
                throw new RuntimeException(
                    'Entry "'
                    . $this->identifierEntry
                    . '" cannot be used as a document identifier for DELETE operation',
                );
            }

            yield (string) $identifier;
        }
    }
}
