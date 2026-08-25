<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\ChartJS;

use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Loader\Closure;
use Flow\ETL\Rows;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;
use Throwable;

use function Flow\Filesystem\DSL\path;
use function implode;
use function iterator_to_array;
use function json_encode;
use function sprintf;
use function str_replace;

final class ChartJSLoader implements Closure, Loader
{
    private ?Path $output = null;

    private ?Filesystem $outputFilesystem = null;

    /**
     * @var null|array<array-key, mixed>
     */
    private ?array $outputVar = null;

    private Path $template;

    private Filesystem $templateFilesystem;

    public function __construct(
        private readonly Chart $type,
    ) {
        $this->template = path(__DIR__ . '/Resources/template/full_page.html');
        $this->templateFilesystem = new NativeLocalFilesystem();
    }

    public function closure(FlowContext $context): void
    {
        if ($this->output === null && $this->outputVar === null) {
            return;
        }

        if ($this->output !== null && $this->outputFilesystem !== null) {
            if ($this->outputFilesystem->status($this->output) !== null) {
                $this->outputFilesystem->rm($this->output);
            }

            $output = $this->outputFilesystem->writeTo($this->output);

            $templateStream = $this->templateFilesystem->readFrom($this->template);

            $template = implode('', iterator_to_array($templateStream->readLines()));
            $templateStream->close();

            $content = str_replace('%_CHART_DATA_%', json_encode($this->type->data(), JSON_THROW_ON_ERROR), $template);

            $output->append($content);

            $output->close();
        }

        if ($this->outputVar !== null) {
            $this->outputVar = $this->type->data();
        }
    }

    public function load(Rows $rows, FlowContext $context): void
    {
        if (!$rows->count()) {
            return;
        }

        $context->telemetry()->loadingStarted($this);

        try {
            $this->type->collect($rows);

            $context->telemetry()->loadingCompleted($this, [TelemetryAttributes::ATTR_LOADING_ROWS => $rows->count()]);
        } catch (Throwable $e) {
            $context->telemetry()->loadingFailed($this, $e);

            throw $e;
        }
    }

    public function withOutputPath(Path $output, Filesystem $filesystem = new NativeLocalFilesystem()): self
    {
        // ChartJSLoader writes through the filesystem directly, so it guards the extension itself
        if (!$output->extension()) {
            throw new RuntimeException('Stream path must have an extension, given: ' . $output->uri());
        }

        if (!$filesystem->supports($output)) {
            throw new InvalidArgumentException(sprintf(
                'Filesystem %s serves "%s://" paths, given: "%s". Pass the filesystem that handles '
                . 'this scheme, e.g. withOutputPath($path, aws_s3_filesystem(...)).',
                $filesystem::class,
                $filesystem->mount()->protocol,
                $output->uri(),
            ));
        }

        $this->outputFilesystem = $filesystem;
        $this->output = $output;

        return $this;
    }

    /**
     * @param array<array-key, mixed> $outputVar
     */
    public function withOutputVar(array &$outputVar): self
    {
        $this->outputVar = &$outputVar;

        return $this;
    }

    public function withTemplate(Path $template, Filesystem $filesystem = new NativeLocalFilesystem()): self
    {
        if (!$filesystem->supports($template)) {
            throw new InvalidArgumentException(sprintf(
                'Filesystem %s serves "%s://" paths, given: "%s". Pass the filesystem that handles '
                . 'this scheme, e.g. withTemplate($path, aws_s3_filesystem(...)).',
                $filesystem::class,
                $filesystem->mount()->protocol,
                $template->uri(),
            ));
        }

        $this->templateFilesystem = $filesystem;
        $this->template = $template;

        return $this;
    }
}
