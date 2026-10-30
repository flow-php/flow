<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Runtime\Executor;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\IO\Loader;
use Flow\ETL\IO\Loader\Closure;
use Flow\ETL\Tests\Double\FakeExtractor;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use SensitiveParameter;

use function Flow\ETL\DSL\data_frame;

final class ClosureTest extends FlowTestCase
{
    public function test_loader_closure(): void
    {
        data_frame()
            ->read(new FakeExtractor(40))
            ->batchSize(2)
            ->write(
                $loader = new class implements Closure, Loader {
                    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
                    {
                        throw InvalidArgumentException::because(
                            '%s holds live state and is never described',
                            self::class,
                        );
                    }

                    public function describe(): ?Description
                    {
                        return null;
                    }

                    public bool $closureCalled = false;

                    public int $rowsLoaded = 0;

                    public function load(Rows $rows, Backend $backend): void
                    {
                        $this->rowsLoaded++;
                    }

                    public function closure(): void
                    {
                        $this->closureCalled = true;
                    }
                },
            )
            ->run();

        static::assertTrue($loader->closureCalled);
        static::assertSame(20, $loader->rowsLoaded);
    }
}
