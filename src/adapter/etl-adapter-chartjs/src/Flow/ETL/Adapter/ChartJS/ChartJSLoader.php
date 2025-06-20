<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\ChartJS;

use Flow\ETL\{FlowContext, Loader, Rows};
use Flow\ETL\Loader\Closure;
use Flow\Filesystem\Path;

final class ChartJSLoader implements Closure, Loader
{
    private ?Path $output = null;

    /**
     * @var null|array<array-key, mixed>
     */
    private ?array $outputVar = null;

    private Path $template;

    public function __construct(private readonly Chart $type)
    {
        $this->template = new Path(__DIR__ . '/Resources/template/full_page.html');
    }

    public function closure(FlowContext $context) : void
    {
        if ($this->output === null && $this->outputVar === null) {
            return;
        }

        if ($this->output !== null) {
            if ($context->streams()->exists($this->output)) {
                $context->streams()->rm($this->output);
            }

            $output = $context->streams()->writeTo($this->output);

            $templateStream = $context->streams()->read($this->template);

            $template = \implode('', \iterator_to_array($templateStream->readLines()));
            $templateStream->close();

            $content = \str_replace(
                '%_CHART_DATA_%',
                \json_encode($this->type->data(), JSON_THROW_ON_ERROR),
                $template
            );

            $output->append($content);

            $context->streams()->closeStreams($this->output);
        }

        if ($this->outputVar !== null) {
            $this->outputVar = $this->type->data();
        }
    }

    public function load(Rows $rows, FlowContext $context) : void
    {
        if (!$rows->count()) {
            return;
        }

        $this->type->collect($rows);
    }

    public function withOutputPath(Path $output) : self
    {
        $this->output = $output;

        return $this;
    }

    /**
     * @param array<array-key, mixed> $outputVar
     */
    public function withOutputVar(array &$outputVar) : self
    {
        $this->outputVar = &$outputVar;

        return $this;
    }

    public function withTemplate(Path $template) : self
    {
        $this->template = $template;

        return $this;
    }
}
