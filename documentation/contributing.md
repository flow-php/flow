# Contributing

[TOC]

Anyone can report a bug or post a feature idea. Only members of the Contributors team can open a pull request.

> Weeks of coding can save you hours of planning.

## Report a bug

- [Bug](https://github.com/flow-php/flow/issues/new?template=bug.yml): existing code does not work as documented.
- [Website](https://github.com/flow-php/flow/issues/new?template=website.yml): a problem on flow-php.com.

## Ask for a feature

Post a [feature idea](https://github.com/flow-php/flow/discussions/new?category=ideas) in GitHub Discussions.
If the Maintainers accept the idea, they create an issue from it and add the issue to the
[Roadmap](https://github.com/orgs/flow-php/projects/1) with the status `Todo`.
If they do not accept it, they close the discussion and give the reason.

To support an idea, upvote the discussion. An issue with the status `Backlog` is not accepted.

- If you are willing to sponsor a feature, see [Sponsor](http://flow-php.com/sponsor/)
- To ask a question, use the [Flow Discord Server](https://discord.gg/5dNXfQyACW)

## Write code

Each pull request resolves one accepted issue. This rule also applies to bug fixes and to small changes.

1. Find an issue with the status `Todo`. If there is none, report the bug or post the idea, and wait for that status.
2. Agree the design with a Maintainer in the issue. Start with the public API, the code that a Flow user will write.
3. If you are not a Contributor, open a
   [Contributor application](https://github.com/flow-php/flow/issues/new?template=contributor.yml).
4. Open the pull request. Put `Resolves: #<issue>` in the description.
5. A Maintainer reviews the pull request and merges it.

## Roles

| Role        | Opens issues | Opens pull requests | Accepts issues, reviews, merges |
|-------------|--------------|---------------------|---------------------------------|
| Everyone    | yes          | no                  | no                              |
| Contributor | yes          | yes                 | no                              |
| Maintainer  | yes          | yes                 | yes                             |

## Roadmap

The [Roadmap](https://github.com/orgs/flow-php/projects/1) shows each issue with a status.

| Status      | Meaning                            |
|-------------|------------------------------------|
| Backlog     | Reported or proposed, not accepted |
| Todo        | Accepted, ready to be worked on    |
| In progress | Someone works on it                |
| In review   | A pull request is open             |
| Done        | Merged or closed                   |

- A priority means that the Maintainers planned the issue. An issue without a priority waits for that.
- A milestone names the release that the issue is planned for. An accepted issue can have no milestone.

Each issue has one type.

| Type    | Meaning                                        |
|---------|------------------------------------------------|
| Bug     | Existing code does not work as documented      |
| Feature | A change that a Flow user can see              |
| Task    | Work that a user cannot see                    |
| Epic    | Groups other issues and has no code of its own |

## Before coding

Read our [Architecture Decision Records](/documentation/adrs.md).
You must follow all of them without exceptions. Only a new ADR can explicitly override one.

## Next Steps

- [Setup development environment](/documentation/contributing/environment.md)
    - [Nix Shell](/documentation/contributing/nix.md)
- [Development Guidelines](/documentation/contributing/guidelines.md)
- [Benchmarks](/documentation/contributing/benchmarks.md)
- Extension Development
    - [Rust: arrow-ext & flow-php-ext](/documentation/contributing/rust.md)
    - [C: pg-query Extension](/documentation/contributing/c.md)
    - [WASM: Interactive Playground](/documentation/contributing/wasm.md)
