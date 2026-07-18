<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Seal;

use CmsIg\Seal\EngineInterface;
use DateTimeInterface;
use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Rows;
use Generator;
use Throwable;

use function array_key_exists;
use function is_int;
use function is_string;

final class SealLoader implements Loader
{
    private int $bulkSize = 100;

    private string $dateFormat = 'Y-m-d';

    private string $dateTimeFormat = DateTimeInterface::ATOM;

    private ?SealEncoder $encoder = null;

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
                    ? $this->encoder()->encode($context->hydrator()->dehydrate($rows))
                    : [],
                $this->operation === Operation::DELETE ? $this->deleteIdentifiers($rows, $context) : [],
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

    public function withDateFormat(string $dateFormat): self
    {
        $this->dateFormat = $dateFormat;

        return $this;
    }

    public function withDateTimeFormat(string $dateTimeFormat): self
    {
        $this->dateTimeFormat = $dateTimeFormat;

        return $this;
    }

    public function withIdentifierEntry(string $entry): self
    {
        $this->identifierEntry = $entry;

        return $this;
    }

    private function encoder(): SealEncoder
    {
        return $this->encoder ??= new SealEncoder($this->dateTimeFormat, $this->dateFormat);
    }

    /**
     * @return Generator<int, string>
     */
    private function deleteIdentifiers(Rows $rows, FlowContext $context): Generator
    {
        foreach ($this->encoder()->encode($context->hydrator()->dehydrate($rows)) as $document) {
            // @mago-ignore analysis:mixed-assignment
            $identifier = array_key_exists($this->identifierEntry, $document)
                ? $document[$this->identifierEntry]
                : null;

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
