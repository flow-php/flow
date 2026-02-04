<?php

declare(strict_types=1);

namespace Flow\ETL\Loader;

use function Flow\ETL\DSL\{df, from_rows};
use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\{FlowContext, Loader, Rows, Transformation};
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Transformer\ScalarFunctionFilterTransformer;

final class BranchingLoader implements Closure, Loader, OverridingLoader
{
    private ?Transformation $transformation = null;

    public function __construct(
        private readonly ScalarFunction $condition,
        private readonly Loader $loader,
    ) {
    }

    public function closure(FlowContext $context) : void
    {
        if ($this->loader instanceof Closure) {
            $this->loader->closure($context);
        }
    }

    public function load(Rows $rows, FlowContext $context) : void
    {
        $context->telemetry()->loadingStarted($this);

        try {
            $rows = (new ScalarFunctionFilterTransformer($this->condition))->transform($rows, $context);

            if ($this->transformation) {
                $rows = df($context->config)
                    ->read(from_rows($rows))
                    ->with($this->transformation)
                    ->fetch();
            }

            $this->loader->load(
                $rows,
                $context
            );

            $context->telemetry()->loadingCompleted($this, [TelemetryAttributes::ATTR_LOADING_ROWS => $rows->count()]);
        } catch (\Throwable $e) {
            $context->telemetry()->loadingFailed($this, $e);

            throw $e;
        }
    }

    public function loaders() : array
    {
        return [
            $this->loader,
        ];
    }

    public function withTransformation(Transformation $transformation) : self
    {
        $this->transformation = $transformation;

        return $this;
    }
}
