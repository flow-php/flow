<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\AST\Transformers;

use Flow\PostgreSql\AST\Transformers\ExplainConfig;
use Flow\PostgreSql\Exception\InvalidExplainConfigException;
use Flow\PostgreSql\QueryBuilder\Utility\ExplainFormat;
use PHPUnit\Framework\TestCase;

final class ExplainConfigTest extends TestCase
{
    public function test_chaining_fluent_methods(): void
    {
        $config = ExplainConfig::forEstimate()
            ->withAnalyze()
            ->withBuffers()
            ->withTiming()
            ->withVerbose()
            ->withFormat(ExplainFormat::TEXT);

        static::assertTrue($config->analyze);
        static::assertTrue($config->buffers);
        static::assertTrue($config->timing);
        static::assertTrue($config->verbose);
        static::assertSame(ExplainFormat::TEXT, $config->format);
    }

    public function test_constructor_throws_when_buffers_enabled_without_analyze(): void
    {
        $this->expectException(InvalidExplainConfigException::class);
        $this->expectExceptionMessage('BUFFERS option requires ANALYZE to be enabled');

        new ExplainConfig(analyze: false, buffers: true);
    }

    public function test_constructor_throws_when_timing_enabled_without_analyze(): void
    {
        $this->expectException(InvalidExplainConfigException::class);
        $this->expectExceptionMessage('TIMING option requires ANALYZE to be enabled');

        new ExplainConfig(analyze: false, buffers: false, timing: true);
    }

    public function test_constructor_throws_when_wal_enabled_without_analyze(): void
    {
        $this->expectException(InvalidExplainConfigException::class);
        $this->expectExceptionMessage('WAL option requires ANALYZE to be enabled');

        new ExplainConfig(analyze: false, buffers: false, timing: false, wal: true);
    }

    public function test_constructor_with_valid_config_without_analyze(): void
    {
        $config = new ExplainConfig(analyze: false, costs: true, buffers: false, timing: false, wal: false);

        static::assertFalse($config->analyze);
        static::assertTrue($config->costs);
        static::assertFalse($config->buffers);
        static::assertFalse($config->timing);
        static::assertFalse($config->wal);
    }

    public function test_for_analysis_can_be_customized_with_fluent_methods(): void
    {
        $config = ExplainConfig::forAnalysis()->withVerbose()->withMemory()->withSettings();

        static::assertTrue($config->verbose);
        static::assertTrue($config->memory);
        static::assertTrue($config->settings);
    }

    public function test_for_analysis_is_static_factory(): void
    {
        $config = ExplainConfig::forAnalysis();

        static::assertTrue($config->analyze);
        static::assertFalse($config->verbose);
        static::assertTrue($config->costs);
        static::assertTrue($config->buffers);
        static::assertTrue($config->timing);
        static::assertTrue($config->summary);
        static::assertFalse($config->memory);
        static::assertFalse($config->settings);
        static::assertFalse($config->wal);
    }

    public function test_for_analysis_returns_config_with_analyze_options(): void
    {
        $config = ExplainConfig::forAnalysis();

        static::assertTrue($config->analyze);
        static::assertTrue($config->costs);
        static::assertTrue($config->buffers);
        static::assertTrue($config->timing);
        static::assertTrue($config->summary);
        static::assertFalse($config->wal);
    }

    public function test_for_estimate_can_be_customized_with_fluent_methods(): void
    {
        $config = ExplainConfig::forEstimate()->withVerbose()->withMemory()->withSettings();

        static::assertTrue($config->verbose);
        static::assertTrue($config->memory);
        static::assertTrue($config->settings);
        static::assertFalse($config->analyze);
    }

    public function test_for_estimate_is_static_factory(): void
    {
        $config = ExplainConfig::forEstimate();

        static::assertFalse($config->analyze);
        static::assertFalse($config->verbose);
        static::assertTrue($config->costs);
        static::assertFalse($config->buffers);
        static::assertFalse($config->timing);
        static::assertFalse($config->summary);
        static::assertFalse($config->memory);
        static::assertFalse($config->settings);
        static::assertFalse($config->wal);
    }

    public function test_for_estimate_returns_config_without_analyze(): void
    {
        $config = ExplainConfig::forEstimate();

        static::assertFalse($config->analyze);
        static::assertTrue($config->costs);
        static::assertFalse($config->buffers);
        static::assertFalse($config->timing);
        static::assertFalse($config->summary);
        static::assertFalse($config->wal);
    }

    public function test_from_array_and_normalize_are_inverse(): void
    {
        $original = new ExplainConfig(
            analyze: true,
            verbose: true,
            costs: true,
            buffers: true,
            timing: true,
            summary: true,
            memory: true,
            settings: true,
            wal: true,
            format: ExplainFormat::TEXT,
        );

        $normalized = $original->normalize();
        $restored = ExplainConfig::fromArray($normalized);

        static::assertEquals($original, $restored);
    }

    public function test_from_array_creates_instance(): void
    {
        $data = [
            'analyze' => true,
            'verbose' => true,
            'costs' => true,
            'buffers' => true,
            'timing' => true,
            'summary' => true,
            'memory' => true,
            'settings' => true,
            'wal' => true,
            'format' => 'text',
        ];

        $config = ExplainConfig::fromArray($data);

        static::assertTrue($config->analyze);
        static::assertTrue($config->verbose);
        static::assertTrue($config->costs);
        static::assertTrue($config->buffers);
        static::assertTrue($config->timing);
        static::assertTrue($config->summary);
        static::assertTrue($config->memory);
        static::assertTrue($config->settings);
        static::assertTrue($config->wal);
        static::assertSame(ExplainFormat::TEXT, $config->format);
    }

    public function test_from_array_validates_config(): void
    {
        $data = [
            'analyze' => false,
            'verbose' => false,
            'costs' => true,
            'buffers' => true,
            'timing' => false,
            'summary' => false,
            'memory' => false,
            'settings' => false,
            'wal' => false,
            'format' => 'json',
        ];

        $this->expectException(InvalidExplainConfigException::class);
        $this->expectExceptionMessage('BUFFERS option requires ANALYZE to be enabled');

        ExplainConfig::fromArray($data);
    }

    public function test_from_array_with_for_estimate_config(): void
    {
        $original = ExplainConfig::forEstimate();
        $normalized = $original->normalize();
        $restored = ExplainConfig::fromArray($normalized);

        static::assertEquals($original, $restored);
    }

    public function test_normalize_returns_all_fields(): void
    {
        $config = ExplainConfig::forAnalysis()
            ->withVerbose()
            ->withMemory()
            ->withSettings()
            ->withWal()
            ->withFormat(ExplainFormat::YAML);

        $normalized = $config->normalize();

        static::assertTrue($normalized['analyze']);
        static::assertTrue($normalized['verbose']);
        static::assertTrue($normalized['costs']);
        static::assertTrue($normalized['buffers']);
        static::assertTrue($normalized['timing']);
        static::assertTrue($normalized['summary']);
        static::assertTrue($normalized['memory']);
        static::assertTrue($normalized['settings']);
        static::assertTrue($normalized['wal']);
        static::assertSame('yaml', $normalized['format']);
    }

    public function test_normalize_returns_expected_keys(): void
    {
        $config = ExplainConfig::forAnalysis();

        $normalized = $config->normalize();

        $expectedKeys = [
            'analyze',
            'verbose',
            'costs',
            'buffers',
            'timing',
            'summary',
            'memory',
            'settings',
            'wal',
            'format',
        ];

        static::assertSame($expectedKeys, \array_keys($normalized));
    }

    public function test_with_analyze_returns_new_instance(): void
    {
        $original = ExplainConfig::forEstimate();
        $modified = $original->withAnalyze();

        static::assertNotSame($original, $modified);
        static::assertFalse($original->analyze);
        static::assertTrue($modified->analyze);
    }

    public function test_with_buffers_requires_analyze(): void
    {
        $config = ExplainConfig::forEstimate();

        $this->expectException(InvalidExplainConfigException::class);
        $this->expectExceptionMessage('BUFFERS option requires ANALYZE to be enabled');

        $config->withBuffers();
    }

    public function test_with_buffers_works_with_analyze(): void
    {
        $config = ExplainConfig::forEstimate();
        $modified = $config->withAnalyze()->withBuffers();

        static::assertTrue($modified->analyze);
        static::assertTrue($modified->buffers);
    }

    public function test_with_costs_returns_new_instance(): void
    {
        $config = ExplainConfig::forAnalysis()->withoutCosts();
        $modified = $config->withCosts();

        static::assertFalse($config->costs);
        static::assertTrue($modified->costs);
    }

    public function test_with_format_returns_new_instance(): void
    {
        $original = ExplainConfig::forAnalysis();
        $modified = $original->withFormat(ExplainFormat::TEXT);

        static::assertSame(ExplainFormat::JSON, $original->format);
        static::assertSame(ExplainFormat::TEXT, $modified->format);
    }

    public function test_with_memory_returns_new_instance(): void
    {
        $original = ExplainConfig::forAnalysis();
        $modified = $original->withMemory();

        static::assertFalse($original->memory);
        static::assertTrue($modified->memory);
    }

    public function test_with_settings_returns_new_instance(): void
    {
        $original = ExplainConfig::forAnalysis();
        $modified = $original->withSettings();

        static::assertFalse($original->settings);
        static::assertTrue($modified->settings);
    }

    public function test_with_summary_returns_new_instance(): void
    {
        $original = ExplainConfig::forEstimate();
        $modified = $original->withSummary();

        static::assertFalse($original->summary);
        static::assertTrue($modified->summary);
    }

    public function test_with_timing_requires_analyze(): void
    {
        $config = ExplainConfig::forEstimate();

        $this->expectException(InvalidExplainConfigException::class);
        $this->expectExceptionMessage('TIMING option requires ANALYZE to be enabled');

        $config->withTiming();
    }

    public function test_with_timing_works_with_analyze(): void
    {
        $config = ExplainConfig::forEstimate();
        $modified = $config->withAnalyze()->withTiming();

        static::assertTrue($modified->analyze);
        static::assertTrue($modified->timing);
    }

    public function test_with_verbose_returns_new_instance(): void
    {
        $original = ExplainConfig::forAnalysis();
        $modified = $original->withVerbose();

        static::assertFalse($original->verbose);
        static::assertTrue($modified->verbose);
    }

    public function test_with_wal_requires_analyze(): void
    {
        $config = ExplainConfig::forEstimate();

        $this->expectException(InvalidExplainConfigException::class);
        $this->expectExceptionMessage('WAL option requires ANALYZE to be enabled');

        $config->withWal();
    }

    public function test_with_wal_works_with_analyze(): void
    {
        $config = ExplainConfig::forAnalysis();
        $modified = $config->withWal();

        static::assertTrue($modified->analyze);
        static::assertTrue($modified->wal);
    }

    public function test_without_analyze_disables_dependent_options(): void
    {
        $original = ExplainConfig::forAnalysis();
        $modified = $original->withoutAnalyze();

        static::assertTrue($original->analyze);
        static::assertTrue($original->buffers);
        static::assertTrue($original->timing);

        static::assertFalse($modified->analyze);
        static::assertFalse($modified->buffers);
        static::assertFalse($modified->timing);
        static::assertFalse($modified->wal);
    }

    public function test_without_buffers_returns_new_instance(): void
    {
        $original = ExplainConfig::forAnalysis();
        $modified = $original->withoutBuffers();

        static::assertTrue($original->buffers);
        static::assertFalse($modified->buffers);
    }

    public function test_without_costs_returns_new_instance(): void
    {
        $original = ExplainConfig::forAnalysis();
        $modified = $original->withoutCosts();

        static::assertTrue($original->costs);
        static::assertFalse($modified->costs);
    }

    public function test_without_memory_returns_new_instance(): void
    {
        $original = ExplainConfig::forAnalysis()->withMemory();
        $modified = $original->withoutMemory();

        static::assertTrue($original->memory);
        static::assertFalse($modified->memory);
    }

    public function test_without_settings_returns_new_instance(): void
    {
        $original = ExplainConfig::forAnalysis()->withSettings();
        $modified = $original->withoutSettings();

        static::assertTrue($original->settings);
        static::assertFalse($modified->settings);
    }

    public function test_without_summary_returns_new_instance(): void
    {
        $original = ExplainConfig::forAnalysis();
        $modified = $original->withoutSummary();

        static::assertTrue($original->summary);
        static::assertFalse($modified->summary);
    }

    public function test_without_timing_returns_new_instance(): void
    {
        $original = ExplainConfig::forAnalysis();
        $modified = $original->withoutTiming();

        static::assertTrue($original->timing);
        static::assertFalse($modified->timing);
    }

    public function test_without_verbose_returns_new_instance(): void
    {
        $original = ExplainConfig::forAnalysis()->withVerbose();
        $modified = $original->withoutVerbose();

        static::assertTrue($original->verbose);
        static::assertFalse($modified->verbose);
    }

    public function test_without_wal_returns_new_instance(): void
    {
        $config = ExplainConfig::forAnalysis()->withWal();
        $modified = $config->withoutWal();

        static::assertTrue($config->wal);
        static::assertFalse($modified->wal);
    }
}
