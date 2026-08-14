<?php

declare(strict_types=1);

namespace Flow\ETL\Loader;

use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\DataFrame;
use Flow\ETL\Exception\LimitReachedException;
use Flow\ETL\Extractor\SwappableRowsExtractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Rows;
use Flow\ETL\Transformation;
use Flow\ETL\Transformer;
use Throwable;

use function Flow\ETL\DSL\df;

final class TransformerLoader implements Closure, Loader, OverridingLoader
{
    private bool $limitReached = false;

    private ?DataFrame $transformationDataFrame = null;

    private SwappableRowsExtractor $transformationRows;

    public function __construct(
        private readonly Transformer|Transformation $transformer,
        private readonly Loader $loader,
    ) {
        $this->transformationRows = new SwappableRowsExtractor();
    }

    public function closure(FlowContext $context): void
    {
        if ($this->loader instanceof Closure) {
            $this->loader->closure($context);
        }

        $this->transformationDataFrame = null;
        $this->transformationRows = new SwappableRowsExtractor();
        $this->limitReached = false;
    }

    public function load(Rows $rows, FlowContext $context): void
    {
        $context->telemetry()->loadingStarted($this);

        try {
            $transformer = $this->transformer;

            if ($transformer instanceof Transformer) {
                // @mago-ignore analysis:invalid-argument,too-many-arguments,possibly-invalid-argument
                $this->loader->load($transformer->transform($rows, $context), $context);
            } else {
                $this->transformationDataFrame ??= $transformer->transform(
                    df($context->config)->from($this->transformationRows),
                );

                if (!$this->transformationRows->stopped()) {
                    $this->transformationRows->swap($rows);

                    foreach ($this->transformationDataFrame->get() as $transformedRows) {
                        $this->loader->load($transformedRows, $context);
                    }
                }
            }

            $context->telemetry()->loadingCompleted($this, [TelemetryAttributes::ATTR_LOADING_ROWS => $rows->count()]);
        } catch (LimitReachedException $e) {
            if (!$this->limitReached) {
                $this->limitReached = true;
                $context->telemetry()->limitReached(['limit' => $e->limit]);
            }

            $context->telemetry()->loadingCompleted($this, [TelemetryAttributes::ATTR_LOADING_ROWS => 0]);
        } catch (Throwable $e) {
            $context->telemetry()->loadingFailed($this, $e);

            throw $e;
        }
    }

    public function loaders(): array
    {
        return [$this->loader];
    }
}
