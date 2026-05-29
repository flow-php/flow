<?php

declare(strict_types=1);

namespace Flow\Website\Model\Documentation;

use function base64_decode;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_mixed;
use function Flow\Types\DSL\type_string;
use function implode;
use function ltrim;
use function rtrim;

final readonly class DSLDefinition
{
    /**
     * @param array<string, mixed> $data
     */
    public function __construct(
        private array $data,
    ) {}

    /**
     * @return array<string, mixed>
     */
    public function data(): array
    {
        return $this->data;
    }

    public function docComment(): string
    {
        if ($this->data['doc_comment'] === null) {
            return '';
        }

        return type_string()->assert(base64_decode(type_string()->assert($this->data['doc_comment']), true));
    }

    /**
     * @return array<Example>
     */
    public function examples(): array
    {
        $examples = [];

        foreach (type_list(type_map(type_string(), type_mixed()))->assert($this->data['attributes']) as $attribute) {
            if ($attribute['name'] === 'DocumentationExample') {
                $arguments = type_map(type_string(), type_string())->assert($attribute['arguments']);

                $examples[] = new Example($arguments['topic'], $arguments['example'], $arguments['option'] ?? null);
            }
        }

        return $examples;
    }

    public function githubUrl(string $version = '1.x'): string
    {
        $startLineInFile = type_integer()->assert($this->data['start_line_in_file']);
        $startLine = $startLineInFile > 0 ? '#L' . $startLineInFile : '';

        return (
            'https://github.com/flow-php/flow/blob/'
            . $version
            . '/'
            . ltrim(type_string()->assert($this->data['repository_path']), '/')
            . $startLine
        );
    }

    public function hasDocComment(): bool
    {
        return $this->data['doc_comment'] !== null;
    }

    public function module(): ?Module
    {
        foreach (type_list(type_map(type_string(), type_mixed()))->assert($this->data['attributes']) as $attribute) {
            if ($attribute['name'] === 'DocumentationDSL') {
                foreach (type_map(type_string(), type_string())->assert(
                    $attribute['arguments'],
                ) as $name => $argument) {
                    if ($name === 'module') {
                        return Module::fromName($argument);
                    }
                }
            }
        }

        return null;
    }

    public function name(): string
    {
        return type_string()->assert($this->data['name']);
    }

    public function path(): string
    {
        return type_string()->assert($this->data['repository_path']) . '/' . type_string()->assert($this->data['name']);
    }

    public function slug(): string
    {
        return type_string()->assert($this->data['slug']);
    }

    public function toString(): string
    {
        if ($this->hasDocComment()) {
            $output = $this->docComment() . PHP_EOL;
        } else {
            $output = '';
        }

        $output .= type_string()->assert($this->data['name']);

        $output .= '(';

        $parameters = [];

        foreach (type_list(type_map(type_string(), type_mixed()))->assert($this->data['parameters']) as $parameter) {
            $parameters[] = $this->parameterToString($parameter);
        }

        $output .= implode(', ', $parameters);

        $output .= ') : ';

        $output .= $this->typeToString(
            type_list(type_map(type_string(), type_mixed()))->assert($this->data['return_type']),
        );

        return $output;
    }

    public function type(): ?Type
    {
        foreach (type_list(type_map(type_string(), type_mixed()))->assert($this->data['attributes']) as $attribute) {
            if ($attribute['name'] === 'DocumentationDSL') {
                foreach (type_map(type_string(), type_string())->assert(
                    $attribute['arguments'],
                ) as $name => $argument) {
                    if ($name === 'type') {
                        return Type::fromName($argument);
                    }
                }
            }
        }

        return null;
    }

    /**
     * @param array<string, mixed> $parameter
     */
    private function parameterToString(array $parameter): string
    {
        return (
            $this->typeToString(type_list(type_map(type_string(), type_mixed()))->assert($parameter['type']))
            . ' $'
            . type_string()->assert($parameter['name'])
        );
    }

    /**
     * @param array<array<string, mixed>> $type
     */
    private function typeToString(array $type): string
    {
        $output = '';

        foreach ($type as $item) {
            $name = type_string()->assert($item['name']);

            if (type_boolean()->assert($item['is_nullable']) && $name !== 'null') {
                $output .= '?';
            }

            $output .= $name . '|';

            if (type_boolean()->assert($item['is_variadic'])) {
                $output .= '...';
            }
        }

        return rtrim($output, '|');
    }
}
