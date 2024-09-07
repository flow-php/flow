<?php

declare(strict_types=1);

namespace Flow\Website\Model\Documentation;

final class DSLDefinition
{
    /**
     * @param array{
     *      repository_path: string,
     *      start_line_in_file: int|false,
     *      slug: string,
     *      name: string,
     *      namespace: string,
     *      parameters: array<mixed>,
     *      return_type: array<mixed>,
     *      attributes: array<mixed>,
     *      doc_comment: string|null,
     *  } $data
     */
    public function __construct(private readonly array $data)
    {
    }

    public function data() : array
    {
        return $this->data;
    }

    public function docComment() : string
    {
        return \base64_decode($this->data['doc_comment'], true);
    }

    /**
     * @return array<Example>
     */
    public function examples() : array
    {
        $examples = [];

        foreach ($this->data['attributes'] as $attribute) {
            if ($attribute['name'] === 'DocumentationExample') {
                $examples[] = new Example(
                    $attribute['arguments']['topic'],
                    $attribute['arguments']['example'],
                );
            }
        }

        return $examples;
    }

    public function githubUrl(string $version = '1.x') : string
    {
        $startLine = $this->data['start_line_in_file'] ? '#L' . $this->data['start_line_in_file'] : '';

        return 'https://github.com/flow-php/flow/blob/' . $version . '/' . \ltrim($this->data['repository_path'], '/') . $startLine;
    }

    public function hasDocComment() : bool
    {
        return $this->data['doc_comment'] !== null;
    }

    public function module() : ?Module
    {
        foreach ($this->data['attributes'] as $attribute) {
            if ($attribute['name'] === 'DocumentationDSL') {
                foreach ($attribute['arguments'] as $name => $argument) {
                    if ($name === 'module') {
                        return Module::fromName($argument);
                    }
                }
            }
        }

        return null;
    }

    public function name() : string
    {
        return $this->data['name'];
    }

    public function path() : string
    {
        return $this->data['repository_path'] . '/' . $this->data['name'];
    }

    public function slug() : string
    {
        return $this->data['slug'];
    }

    public function toString() : string
    {
        if ($this->hasDocComment()) {
            $output = $this->docComment() . PHP_EOL;
        } else {
            $output = '';
        }

        $output .= $this->data['name'];

        $output .= '(';

        $parameters = [];

        foreach ($this->data['parameters'] as $parameter) {
            $parameters[] = $this->parameterToString($parameter);
        }

        $output .= \implode(', ', $parameters);

        $output .= ') : ';

        $output .= $this->typeToString($this->data['return_type']);

        return $output;
    }

    public function type() : ?Type
    {
        foreach ($this->data['attributes'] as $attribute) {
            if ($attribute['name'] === 'DocumentationDSL') {
                foreach ($attribute['arguments'] as $name => $argument) {
                    if ($name === 'type') {
                        return Type::fromName($argument);
                    }
                }
            }
        }

        return null;
    }

    private function parameterToString(array $parameter) : string
    {
        $output = $this->typeToString($parameter['type']);
        $output .= ' $' . $parameter['name'];

        return $output;
    }

    private function typeToString(array $type) : string
    {
        $output = '';

        foreach ($type as $item) {
            if ($item['is_nullable'] && $item['name'] !== 'null') {
                $output .= '?';
            }

            $output .= $item['name'] . '|';

            if ($item['is_variadic']) {
                $output .= '...';
            }
        }

        return \rtrim($output, '|');
    }
}
