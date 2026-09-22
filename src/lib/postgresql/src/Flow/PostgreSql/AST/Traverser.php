<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST;

use Flow\PostgreSql\Exception\ParserException;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\ParseResult;
use Google\Protobuf\DescriptorPool;
use Google\Protobuf\Field\Kind;
use Google\Protobuf\Internal\Message;
use Google\Protobuf\RepeatedField;

use function array_filter;
use function array_key_exists;
use function array_pop;
use function array_values;
use function count;
use function in_array;
use function is_object;
use function iterator_to_array;
use function sprintf;
use function str_replace;
use function ucwords;

/**
 * AST Traverser for PostgreSQL parse trees.
 *
 * Walks every message-typed field of every node in protobuf descriptor order and calls registered visitors and
 * modifiers for specific node types.
 * Visitors receive nodes for read-only operations (collection, analysis).
 * Modifiers receive nodes with context for mutation operations.
 * Handlers registered for ParseResult receive the whole query once, before its statements.
 */
final class Traverser
{
    /**
     * @var array<string, string> snake field name → accessor suffix
     */
    private static array $accessorSuffixes = [];

    /**
     * @var array<class-string<Message>, list<array{0: string, 1: bool}>> accessor suffix, repeated
     */
    private static array $messageFields = [];

    /**
     * @var list<Message>
     */
    private array $ancestorStack = [];

    /**
     * @var array<class-string, array<NodeModifier>>
     */
    private readonly array $modifiers;

    private ParseResult $parseResult;

    private bool $stopTraversal = false;

    /**
     * @var array<class-string, array<NodeVisitor>>
     */
    private readonly array $visitors;

    public function __construct(NodeVisitor|NodeModifier ...$handlers)
    {
        $visitors = [];
        $modifiers = [];

        foreach ($handlers as $handler) {
            foreach ($handler::nodeClasses() as $nodeClass) {
                if ($handler instanceof NodeModifier) {
                    $modifiers[$nodeClass][] = $handler;
                }

                if ($handler instanceof NodeVisitor) {
                    $visitors[$nodeClass][] = $handler;
                }
            }
        }

        $this->visitors = $visitors;
        $this->modifiers = $modifiers;
    }

    private static function accessorSuffix(string $snake): string
    {
        return self::$accessorSuffixes[$snake] ??= str_replace('_', '', ucwords($snake, '_'));
    }

    /**
     * @param class-string<Message> $class
     *
     * @return list<array{0: string, 1: bool}>
     */
    private static function messageFields(string $class): array
    {
        if (array_key_exists($class, self::$messageFields)) {
            return self::$messageFields[$class];
        }

        $descriptor = DescriptorPool::getGeneratedPool()->getDescriptorByClassName($class);
        $fields = [];

        for ($i = 0, $count = $descriptor->getFieldCount(); $i < $count; $i++) {
            $field = $descriptor->getField($i);

            if ($field->getType() === Kind::TYPE_MESSAGE && !$field->isMap()) {
                $fields[] = [self::accessorSuffix($field->getName()), $field->isRepeated()];
            }
        }

        return self::$messageFields[$class] = $fields;
    }

    /**
     * Traverse a ParseResult.
     */
    public function traverse(ParseResult $parseResult): void
    {
        $this->parseResult = $parseResult;
        $this->stopTraversal = false;
        $this->ancestorStack = [];

        $traverseStatements = true;

        foreach ($this->modifiers[ParseResult::class] ?? [] as $modifier) {
            $result = $modifier->modify($parseResult, new ModificationContext([], 0, $parseResult));

            if ($result === NodeModifier::STOP_TRAVERSAL) {
                return;
            }

            if ($result === NodeModifier::DONT_TRAVERSE_CHILDREN) {
                $traverseStatements = false;
            }
        }

        foreach ($this->visitors[ParseResult::class] ?? [] as $visitor) {
            $result = $visitor->enter($parseResult);

            if ($result === NodeVisitor::STOP_TRAVERSAL) {
                return;
            }

            if ($result === NodeVisitor::DONT_TRAVERSE_CHILDREN) {
                $traverseStatements = false;
            }
        }

        if ($traverseStatements) {
            foreach ($parseResult->getStmts() as $rawStmt) {
                $stmt = $rawStmt->getStmt();

                if ($stmt === null) {
                    continue;
                }

                $this->visit($rawStmt, 'Stmt', null, $stmt, 1);

                if ($this->stopTraversal) {
                    return;
                }
            }
        }

        foreach ($this->visitors[ParseResult::class] ?? [] as $visitor) {
            $visitor->leave($parseResult);
        }
    }

    /**
     * @return bool true when $value must be dropped from its list slot
     */
    private function visit(Message $owner, string $field, ?int $index, Message $value, int $depth): bool
    {
        $inner = $value;

        if ($value instanceof Node) {
            $which = $value->getNode();

            if ($which === '') {
                return false;
            }

            /** @var Message $inner */
            // @mago-expect analysis:string-member-selector
            $inner = $value->{'get' . self::accessorSuffix($which)}();
        }

        $context = new ModificationContext($this->ancestorStack, $depth, $this->parseResult);
        $skipChildren = false;

        foreach ($this->modifiers[$inner::class] ?? [] as $modifier) {
            $result = $modifier->modify($inner, $context);

            if ($result === NodeModifier::STOP_TRAVERSAL) {
                $this->stopTraversal = true;

                return false;
            }

            if ($result === NodeModifier::REMOVE_NODE) {
                if ($index === null) {
                    throw new ParserException(sprintf(
                        'REMOVE_NODE is allowed only in a list, %s::%s is a single node',
                        $owner::class,
                        $field,
                    ));
                }

                return true;
            }

            if (is_object($result)) {
                $allowed = $value instanceof Node ? $result instanceof Node : $result instanceof $inner;

                if (!$allowed) {
                    throw new ParserException(sprintf(
                        '%s cannot replace a node in %s::%s',
                        $result::class,
                        $owner::class,
                        $field,
                    ));
                }

                if ($index === null) {
                    // @mago-expect analysis:string-member-selector
                    $owner->{'set' . $field}($result);
                } else {
                    // @mago-expect analysis:string-member-selector
                    $owner->{'get' . $field}()[$index] = $result;
                }

                return false;
            }

            if ($result === NodeModifier::DONT_TRAVERSE_CHILDREN) {
                $skipChildren = true;
            }
        }

        foreach ($this->visitors[$inner::class] ?? [] as $visitor) {
            $result = $visitor->enter($inner);

            if ($result === NodeVisitor::STOP_TRAVERSAL) {
                $this->stopTraversal = true;

                return false;
            }

            if ($result === NodeVisitor::DONT_TRAVERSE_CHILDREN) {
                $skipChildren = true;
            }
        }

        if (!$skipChildren) {
            $this->ancestorStack[] = $inner;
            $this->visitChildren($inner, $depth);
            array_pop($this->ancestorStack);

            if ($this->stopTraversal) {
                return false;
            }
        }

        foreach ($this->visitors[$inner::class] ?? [] as $visitor) {
            if ($visitor->leave($inner) === NodeVisitor::STOP_TRAVERSAL) {
                $this->stopTraversal = true;

                return false;
            }
        }

        return false;
    }

    private function visitChildren(Message $message, int $depth): void
    {
        foreach (self::messageFields($message::class) as [$suffix, $repeated]) {
            if (!$repeated) {
                /** @var null|Message $child */
                // @mago-expect analysis:string-member-selector
                $child = $message->{'get' . $suffix}();

                if ($child !== null) {
                    $this->visit($message, $suffix, null, $child, $depth + 1);
                }
            } else {
                /** @var RepeatedField<Message> $children */
                // @mago-expect analysis:string-member-selector
                $children = $message->{'get' . $suffix}();
                $drop = [];

                for ($i = 0, $count = count($children); $i < $count; $i++) {
                    if ($this->visit($message, $suffix, $i, $children[$i], $depth + 1)) {
                        $drop[] = $i;
                    }

                    if ($this->stopTraversal) {
                        break;
                    }
                }

                // Rebuilt through the setter (never offsetUnset) so the C extension and pure PHP protobuf agree;
                // applied even after STOP, because the removal was already decided.
                if ($drop !== []) {
                    // @mago-expect analysis:string-member-selector
                    $message->{'set' . $suffix}(array_values(array_filter(
                        iterator_to_array($children),
                        static fn(int|string $i): bool => !in_array($i, $drop, true),
                        ARRAY_FILTER_USE_KEY,
                    )));
                }
            }

            if ($this->stopTraversal) {
                return;
            }
        }
    }
}
