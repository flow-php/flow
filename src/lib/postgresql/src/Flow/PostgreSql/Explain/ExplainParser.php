<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Explain;

use Flow\PostgreSql\Exception\ExplainParseException;
use Flow\PostgreSql\Explain\Plan\{Buffers, Cost, Plan, PlanNode, PlanNodeType, Timing};

final readonly class ExplainParser
{
    public function parse(string $jsonOutput) : Plan
    {
        $data = \json_decode($jsonOutput, true);

        if (\json_last_error() !== JSON_ERROR_NONE) {
            throw ExplainParseException::invalidJson(\json_last_error_msg());
        }

        if (!\is_array($data)) {
            throw ExplainParseException::unexpectedFormat('array', \gettype($data));
        }

        if (!\array_key_exists(0, $data)) {
            throw ExplainParseException::unexpectedFormat('non-empty array', 'empty array');
        }

        /** @var array<string, mixed> $result */
        $result = $data[0];

        if (!\is_array($result) || !\array_key_exists('Plan', $result) || !\is_array($result['Plan'])) {
            throw ExplainParseException::missingField('Plan');
        }

        /** @var array<string, mixed> $planData */
        $planData = $result['Plan'];
        $rootNode = $this->parseNode($planData);

        return new Plan(
            $rootNode,
            \array_key_exists('Planning Time', $result) && \is_numeric($result['Planning Time']) ? (float) $result['Planning Time'] : null,
            \array_key_exists('Execution Time', $result) && \is_numeric($result['Execution Time']) ? (float) $result['Execution Time'] : null,
            \array_key_exists('Memory Used', $result) && \is_numeric($result['Memory Used']) ? (int) $result['Memory Used'] : null,
            \array_key_exists('Memory Peak', $result) && \is_numeric($result['Memory Peak']) ? (int) $result['Memory Peak'] : null,
        );
    }

    /**
     * @param array<string, mixed> $nodeData
     */
    private function parseBuffers(array $nodeData) : ?Buffers
    {
        $hasBuffers = \array_key_exists('Shared Hit Blocks', $nodeData)
            || \array_key_exists('Shared Read Blocks', $nodeData)
            || \array_key_exists('Temp Read Blocks', $nodeData)
            || \array_key_exists('Temp Written Blocks', $nodeData);

        if (!$hasBuffers) {
            return null;
        }

        return new Buffers(
            $this->toInt($nodeData['Shared Hit Blocks'] ?? 0),
            $this->toInt($nodeData['Shared Read Blocks'] ?? 0),
            $this->toInt($nodeData['Shared Dirtied Blocks'] ?? 0),
            $this->toInt($nodeData['Shared Written Blocks'] ?? 0),
            $this->toInt($nodeData['Local Hit Blocks'] ?? 0),
            $this->toInt($nodeData['Local Read Blocks'] ?? 0),
            $this->toInt($nodeData['Local Dirtied Blocks'] ?? 0),
            $this->toInt($nodeData['Local Written Blocks'] ?? 0),
            $this->toInt($nodeData['Temp Read Blocks'] ?? 0),
            $this->toInt($nodeData['Temp Written Blocks'] ?? 0),
        );
    }

    /**
     * @param array<string, mixed> $nodeData
     */
    private function parseCost(array $nodeData) : Cost
    {
        return new Cost(
            $this->toFloat($nodeData['Startup Cost'] ?? 0.0),
            $this->toFloat($nodeData['Total Cost'] ?? 0.0),
        );
    }

    /**
     * @param array<string, mixed> $nodeData
     */
    private function parseNode(array $nodeData) : PlanNode
    {
        $nodeType = PlanNodeType::fromString($this->toString($nodeData['Node Type'] ?? 'Unknown'));

        $children = [];

        if (\array_key_exists('Plans', $nodeData) && \is_array($nodeData['Plans'])) {
            foreach ($nodeData['Plans'] as $childData) {
                if (\is_array($childData)) {
                    /** @var array<string, mixed> $childData */
                    $children[] = $this->parseNode($childData);
                }
            }
        }

        $sortKey = null;

        if (\array_key_exists('Sort Key', $nodeData) && \is_array($nodeData['Sort Key'])) {
            $sortKey = \implode(', ', \array_map($this->toString(...), $nodeData['Sort Key']));
        }

        return new PlanNode(
            $nodeType,
            $this->parseCost($nodeData),
            $this->toInt($nodeData['Plan Rows'] ?? 0),
            $this->toInt($nodeData['Plan Width'] ?? 0),
            $children,
            \array_key_exists('Relation Name', $nodeData) ? $this->toString($nodeData['Relation Name']) : null,
            \array_key_exists('Schema', $nodeData) ? $this->toString($nodeData['Schema']) : null,
            \array_key_exists('Alias', $nodeData) ? $this->toString($nodeData['Alias']) : null,
            \array_key_exists('Index Name', $nodeData) ? $this->toString($nodeData['Index Name']) : null,
            \array_key_exists('Index Cond', $nodeData) ? $this->toString($nodeData['Index Cond']) : null,
            \array_key_exists('Filter', $nodeData) ? $this->toString($nodeData['Filter']) : null,
            $this->parseTiming($nodeData),
            $this->parseBuffers($nodeData),
            \array_key_exists('Actual Rows', $nodeData) ? $this->toInt($nodeData['Actual Rows']) : null,
            \array_key_exists('Actual Loops', $nodeData) ? $this->toInt($nodeData['Actual Loops']) : null,
            \array_key_exists('Rows Removed by Filter', $nodeData) ? $this->toInt($nodeData['Rows Removed by Filter']) : null,
            \array_key_exists('Rows Removed by Index Recheck', $nodeData) ? $this->toInt($nodeData['Rows Removed by Index Recheck']) : null,
            \array_key_exists('Parent Relationship', $nodeData) ? $this->toString($nodeData['Parent Relationship']) : null,
            \array_key_exists('Scan Direction', $nodeData) ? $this->toString($nodeData['Scan Direction']) : null,
            \array_key_exists('Join Type', $nodeData) ? $this->toString($nodeData['Join Type']) : null,
            \array_key_exists('Hash Cond', $nodeData) ? $this->toString($nodeData['Hash Cond']) : null,
            $sortKey,
            \array_key_exists('Sort Method', $nodeData) ? $this->toString($nodeData['Sort Method']) : null,
            \array_key_exists('Sort Space Used', $nodeData) ? $this->toInt($nodeData['Sort Space Used']) : null,
            \array_key_exists('Sort Space Type', $nodeData) ? $this->toString($nodeData['Sort Space Type']) : null,
            $nodeData,
        );
    }

    /**
     * @param array<string, mixed> $nodeData
     */
    private function parseTiming(array $nodeData) : ?Timing
    {
        if (!\array_key_exists('Actual Startup Time', $nodeData) && !\array_key_exists('Actual Total Time', $nodeData)) {
            return null;
        }

        return new Timing(
            $this->toFloat($nodeData['Actual Startup Time'] ?? 0.0),
            $this->toFloat($nodeData['Actual Total Time'] ?? 0.0),
            $this->toInt($nodeData['Actual Loops'] ?? 1),
        );
    }

    private function toFloat(mixed $value) : float
    {
        return \is_numeric($value) ? (float) $value : 0.0;
    }

    private function toInt(mixed $value) : int
    {
        return \is_numeric($value) ? (int) $value : 0;
    }

    private function toString(mixed $value) : string
    {
        return \is_scalar($value) ? (string) $value : '';
    }
}
