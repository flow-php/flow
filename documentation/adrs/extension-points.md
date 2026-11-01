# Extension Points

[TOC]

Proposed by: @norberttech  
Date: 2025-01-09

## Context
---

Flow is a framework. As all frameworks, it must give developers extension points, where they can connect to the framework and extend it.

How to find an extension point? 

Look for interfaces. An example is `\Flow\ETL\Storage\RowsStorage`, an interface with a few implementations.
Because of the interface, you can create your own implementation and use it in the framework.

Most of the extension points are in `\Flow\ETL\Runtime\Config`. There, you can replace the default
implementations of different interfaces with implementations that fit your needs better.

Those extension points exist by design, and they must follow the backward compatibility policy.

Are there other extension points to think about?

Yes. Each class that is not marked final, and that goes into the DataFrame in some way, is also an extension
point.

The reason: nothing stops developers from extending those classes and putting their custom version into the
DataFrame.

Our backward compatibility policy cannot cover those extension points.

## Decision
---

Closed by default:

All classes must be marked as `final`. Each extension point that we need must be an interface.

Always possible to open later:

In justified cases, we can give extension points through abstract classes, or allow people to extend the class. One
of the core contributors must always approve these cases.

No guarantee and no support for workarounds:

Developers can still use reflection or special tools to "unfinalize" Flow classes. We do not guarantee backward
compatibility for those cases, and each developer does it at their own risk. As Flow PHP maintainers, we also keep the
right to give no help or support for those cases.

## Pros & Cons
---

- no risk to break the backward compatibility promise by accident
- more predictable behavior
- lower cost to keep backward compatibility
- easier to find the real extension points
- impossible to mock classes that are marked as `final` in tests (which is good, because users must not mock Flow classes in their test suites)

## Alternatives Considered (optional)
---

An alternative is the approach of Symfony: mark classes as `final`
[when it makes sense](https://github.com/symfony/symfony/issues/15233#issuecomment-1566682054).
But this approach is too easy to abuse.

Also, a change from a non-final class to a final class is a much bigger BC break
than the other way around.

## Links and References (optional)
---

- [When to declare classes final](https://ocramius.github.io/blog/when-to-declare-classes-final/)
- [Final Classes by default, why?](https://matthiasnoback.nl/2018/09/final-classes-by-default-why/)