<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\ChartJS\IO\Loader;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\IO\Loader;
use Flow\ETL\IO\Loader\Closure;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;
use SensitiveParameter;

use function implode;
use function iterator_to_array;
use function json_encode;
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
        $this->template = Path::from(__DIR__ . '/../../Resources/template/full_page.html');
        $this->templateFilesystem = new NativeLocalFilesystem();
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        throw InvalidArgumentException::because('%s holds live state and is never described', self::class);
    }

    public function describe(): ?Description
    {
        return null;
    }

    public function closure(): void
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

            $output->append(str_replace(
                '%_CHART_DATA_%',
                json_encode($this->type->data(), JSON_THROW_ON_ERROR),
                $template,
            ));

            $output->close();
        }

        if ($this->outputVar !== null) {
            $this->outputVar = $this->type->data();
        }
    }

    public function load(Rows $rows, Backend $backend): void
    {
        if (!$rows->count()) {
            return;
        }

        $this->type->collect($rows);
    }

    public function withOutputPath(Path $output, Filesystem $filesystem = new NativeLocalFilesystem()): self
    {
        // ChartJSLoader writes through the filesystem directly, so it guards the extension itself
        if (!$output->extension()) {
            throw new RuntimeException('Stream path must have an extension, given: ' . $output->uri());
        }

        $filesystem->supports($output)
            || throw InvalidArgumentException::unsupportedPath($filesystem, $output, 'withOutputPath');

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
        $filesystem->supports($template)
            || throw InvalidArgumentException::unsupportedPath($filesystem, $template, 'withTemplate');

        $this->templateFilesystem = $filesystem;
        $this->template = $template;

        return $this;
    }
}
